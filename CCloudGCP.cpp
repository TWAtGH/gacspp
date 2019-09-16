#include <cassert>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "json.hpp"

#include "CConfigManager.hpp"
#include "CCloudGCP.hpp"
#include "CNetworkLink.hpp"
#include "SFile.hpp"

#include <fstream>

namespace gcp
{
	ICloudFactory* CCloudFactory::mInstance = new CCloudFactory;

    CCloudBill::CCloudBill(double storageCost, double networkCost, double traffic, double operationCost)
        : mStorageCost(storageCost),
          mNetworkCost(networkCost),
          mTraffic(traffic),
          mOperationCost(operationCost)
    {}

    std::string CCloudBill::ToString() const
    {
        std::stringstream res;
        res << std::fixed << std::setprecision(2);
        res << std::setw(12) << "Storage: " << mStorageCost << " CHF" << std::endl;
        res << std::setw(12) << "Network: " << mNetworkCost << " CHF (" << mTraffic << " GiB)" << std::endl;
        res << std::setw(12) << "Operations: " << mOperationCost << " CHF" << std::endl;
        return res.str();
    }


    void CBucket::OnOperation(const CStorageElement::OPERATION op)
    {
        CStorageElement::OnOperation(op);
        switch(op)
        {
            case CStorageElement::INSERT:
                mOperationCosts += 0.000005;
            break;
            case CStorageElement::GET:
                mOperationCosts += 0.0000004;
            break;
            default: break;
        }
    }

    void CBucket::OnIncreaseReplica(std::uint64_t amount, TickType now)
    {
        if (now > mTimeLastCostUpdate)
        {
            mStorageCosts += BYTES_TO_GiB(mUsedStorage) * GetCurStoragePrice() * SECONDS_TO_MONTHS((now - mTimeLastCostUpdate));
            mTimeLastCostUpdate = now;
        }
        CStorageElement::OnIncreaseReplica(amount, now);
    }

    void CBucket::OnRemoveReplica(const SReplica* replica, TickType now)
    {
        std::unique_lock<std::mutex> lock(mReplicaRemoveMutex);
        if (now > mTimeLastCostUpdate)
        {
            mStorageCosts += BYTES_TO_GiB(mUsedStorage) * GetCurStoragePrice() * SECONDS_TO_MONTHS((now - mTimeLastCostUpdate));
            mTimeLastCostUpdate = now;
        }
        CStorageElement::OnRemoveReplica(replica, now, false);
    }

    auto CBucket::CalculateStorageCosts(TickType now) -> double
    {
        if (now > mTimeLastCostUpdate)
        {
            mStorageCosts += BYTES_TO_GiB(mUsedStorage) * GetCurStoragePrice() * SECONDS_TO_MONTHS((now - mTimeLastCostUpdate));
            mTimeLastCostUpdate = now;
        }

        double costs = mStorageCosts;
        mStorageCosts = 0;
        return costs;
    }

    auto CBucket::CalculateOperationCosts(TickType now) -> double
    {
        (void)now;
        double costs = mOperationCosts;
        mOperationCosts = 0;
        return costs;
    }

    auto CBucket::GetCurStoragePrice() const -> double
    {
		const CRegion* region = dynamic_cast<const CRegion*>(GetSite());
		assert(region);
		const TieredPriceType& storagePrice = region->mStoragePrice;
		assert(!storagePrice.empty());

		std::size_t rateIdx = 0, i = 1;
		while( (i < storagePrice.size()) && (mUsedStorage > storagePrice[i].first) )
		{
			++i;
			++rateIdx;
		}
		return static_cast<double>(region->mStoragePrice[rateIdx].second) / 1000000000.0;
    }



    static double CalculateNetworkCostsRecursive(std::uint64_t traffic, TieredPriceType::const_iterator curLevelIt, const TieredPriceType::const_iterator &endIt, std::uint64_t prevThreshold = 0)
    {
	    assert(curLevelIt->first >= prevThreshold);
	    const std::uint64_t threshold = curLevelIt->first - prevThreshold;
		TieredPriceType::const_iterator nextLevelIt = curLevelIt + 1;
	    if (traffic <= threshold || nextLevelIt == endIt)
		    return BYTES_TO_GiB(traffic) * curLevelIt->second;
	    const double lowerLevelCosts = CalculateNetworkCostsRecursive(traffic - threshold, nextLevelIt, endIt, curLevelIt->first);
	    return (BYTES_TO_GiB(threshold) * curLevelIt->second) + lowerLevelCosts;
    }

    auto CRegion::CreateStorageElement(std::string&& name) -> CBucket*
    {
	    CBucket* newBucket = new CBucket(std::move(name), this);
	    mStorageElements.emplace_back(newBucket);
	    return newBucket;
    }

    double CRegion::CalculateStorageCosts(TickType now)
    {
	    double regionStorageCosts = 0;
	    for (const std::unique_ptr<CBucket>& bucket : mStorageElements)
		    regionStorageCosts += bucket->CalculateStorageCosts(now);
	    return regionStorageCosts;
    }

    double CRegion::CalculateOperationCosts(TickType now)
    {
	    double regionOperationCosts = 0;
	    for (const std::unique_ptr<CBucket>& bucket : mStorageElements)
		    regionOperationCosts += bucket->CalculateOperationCosts(now);
	    return regionOperationCosts;
    }

    double CRegion::CalculateNetworkCosts(double& sumUsedTraffic, std::uint64_t& sumDoneTransfers)
    {
	    double regionNetworkCosts = 0;
	    for (const std::unique_ptr<CNetworkLink>& networkLink : mNetworkLinks)
	    {
			const TieredPriceType& networkPrice = mNetworkLinkIdToPrice.at(networkLink->GetId());
            double costs = CalculateNetworkCostsRecursive(networkLink->mUsedTraffic, networkPrice.cbegin(), networkPrice.cend());

            regionNetworkCosts += costs;
            sumUsedTraffic += BYTES_TO_GiB(networkLink->mUsedTraffic);
            sumDoneTransfers += networkLink->mDoneTransfers;
			networkLink->mUsedTraffic = 0;
			networkLink->mDoneTransfers = 0;
			networkLink->mFailedTransfers = 0;
	    }
	    return regionNetworkCosts;
    }



    CCloud::~CCloud()
    {
        if(mSKUSettings)
        {
            delete mSKUSettings;
            mSKUSettings = nullptr;
        }
		if (mNetworkPrices)
		{
			delete mNetworkPrices;
			mNetworkPrices = nullptr;
		}
    }

    auto CCloud::CreateRegion(  std::string&& name,
                                std::string&& locationName,
                                const std::uint8_t multiLocationIdx) -> CRegion*
    {
	    CRegion* newRegion = new CRegion(std::move(name), std::move(locationName), multiLocationIdx);
	    mRegions.emplace_back(newRegion);
	    return newRegion;
    }

    auto CCloud::ProcessBilling(TickType now) -> std::unique_ptr<ICloudBill>
    {
	    double totalStorageCosts = 0;
	    double totalOperationCosts = 0;
	    double totalNetworkCosts = 0;
        double sumUsedTraffic = 0;
        std::uint64_t sumDoneTransfer = 0;
	    for (const std::unique_ptr<ISite>& site : mRegions)
	    {
		    auto region = dynamic_cast<CRegion*>(site.get());
		    assert(region != nullptr);
		    const double regionStorageCosts = region->CalculateStorageCosts(now);
		    const double regionOperationCosts = region->CalculateOperationCosts(now);
		    const double regionNetworkCosts = region->CalculateNetworkCosts(sumUsedTraffic, sumDoneTransfer);
		    totalStorageCosts += regionStorageCosts;
		    totalOperationCosts += regionOperationCosts;
		    totalNetworkCosts += regionNetworkCosts;
	    }
        return std::make_unique<CCloudBill>(totalStorageCosts, totalNetworkCosts, sumUsedTraffic, totalOperationCosts);
    }

    void CCloud::SetupDefaultCloud()
    {
		for (const std::unique_ptr<ISite>& srcSite : mRegions)
		{
			CRegion* srcRegion = dynamic_cast<CRegion*>(srcSite.get());
			assert(srcRegion != nullptr);

			for (const std::unique_ptr<CNetworkLink>& networkLink : srcRegion->mNetworkLinks)
			{
				const ISite* dstSite = networkLink->GetDstSite();
				const std::string dstRegionMultiLocationIdx = std::to_string(dstSite->GetMultiLocationIdx());
				const CRegion* dstRegion = dynamic_cast<const CRegion*>(dstSite);
				std::string skuId;
				if (dstRegion)
				{
					const std::string srcRegionMultiLocationIdx = std::to_string(srcSite->GetMultiLocationIdx());
					mNetworkPrices->at("interregion").at(srcRegionMultiLocationIdx).at(dstRegionMultiLocationIdx).at("skuId").get_to(skuId);
				}
				else
					mNetworkPrices->at("download").at(dstRegionMultiLocationIdx).at("skuId").get_to(skuId);
				
				srcRegion->mNetworkLinkIdToPrice[networkLink->GetId()] = GetTieredRateFromSKUId(skuId);
			}
		}
    }

    bool CCloud::LoadConfig(const json& config)
    {
		if (!config.contains("gcp"))
			return false;

		const json& gcpCfgJson = config["gcp"];

		try
        {
			json skuIdsJson = gcpCfgJson.at("skuIds");
			if(skuIdsJson.contains(JSON_FILE_IMPORT_KEY))
			{
				std::filesystem::path filePath = skuIdsJson.at(JSON_FILE_IMPORT_KEY).get<std::string>();
				skuIdsJson.clear();
				CConfigManager::GetRef().TryLoadCfg(skuIdsJson, filePath);
			}

			mSKUSettings = new json;
			for (const json& skuJson : skuIdsJson.at("skus"))
			{
				try
				{
					std::string skuId = skuJson.at("skuId").get<std::string>();
					if (mSKUSettings->count(skuId) > 0)
					{
						std::cout << "Ignoring second object for same SKU ID: " << skuId << std::endl;
						continue;
					}
					(*mSKUSettings)[skuId] = skuJson;
				}
				catch(const json::out_of_range& error)
				{
					std::cout << "Failed to find skuId for object: " << error.what() << std::endl;
				}
			}
        }
		catch(const json::exception& error)
		{
			std::cout << "Failed to load sku ids config: " << error.what() << std::endl;
			return false;
		}

		try
		{
			json networkPricesJson = gcpCfgJson.at("networkPrices");
			if (networkPricesJson.contains(JSON_FILE_IMPORT_KEY))
			{
				std::filesystem::path filePath = networkPricesJson.at(JSON_FILE_IMPORT_KEY).get<std::string>();
				networkPricesJson.clear();
				CConfigManager::GetRef().TryLoadCfg(networkPricesJson, filePath);
			}

			mNetworkPrices = new json(networkPricesJson);
		}
		catch (const json::exception& error)
		{
			std::cout << "Failed to load sku ids config: " << error.what() << std::endl;
			return false;
		}

		try
		{
			for (const json& regionJson : gcpCfgJson.at("regions"))
			{
				std::unordered_map<std::string, std::string> customConfig;
				CRegion* region = nullptr;
				try
				{
					region = CreateRegion(regionJson.at("name").get<std::string>(),
						regionJson.at("location").get<std::string>(),
						regionJson.at("multiLocationIdx").get<std::uint8_t>());

					region->mStoragePrice = GetTieredRateFromSKUId(regionJson.at("storageSKUId").get<std::string>());

					for (const auto& [regionJsonKey, regionJsonValue] : regionJson.items())
					{
						if (regionJsonKey == "buckets")
						{
							assert(regionJsonValue.is_array());
							for (const json& bucketJson : regionJsonValue)
							{
								try
								{
									CBucket* bucket = region->CreateStorageElement(bucketJson.at("name").get<std::string>());
								}
								catch (const json::out_of_range& error)
								{
									std::cout << "Failed to add bucket: " << error.what() << std::endl;
									continue;
								}
							}
						}
						else if ((regionJsonKey == "name") || (regionJsonKey == "location") || (regionJsonKey == "multiLocationIdx"))
							continue;
						else if (regionJsonValue.type() == json::value_t::string)
							customConfig[regionJsonKey] = regionJsonValue.get<std::string>();
						else
							customConfig[regionJsonKey] = regionJsonValue.dump();
					}
					region->mCustomConfig = std::move(customConfig);
				}
				catch (const json::out_of_range& error)
				{
					std::cout << "Failed to add region: " << error.what() << std::endl;
					continue;
				}
			}
		}
		catch (const json::exception& error)
		{
			std::cout << "Failed to load regions: " << error.what() << std::endl;
			return false;
		}

		return true;
    }

	auto CCloud::GetTieredRateFromSKUId(const std::string& skuId) const -> TieredPriceType
	{
		//<skuID> -> pricingInfo -> pricingExpression -> tieredRates: startUsageAmount; unitPrice -> nanos
		TieredPriceType prices;

		try
		{
			const json& pricingJson = mSKUSettings->at(skuId).at("pricingInfo").at(0).at("pricingExpression");

			const std::string& usageUnit = pricingJson.at("usageUnit").get<std::string>();
			long double baseUnitConversionFactor = pricingJson.at("baseUnitConversionFactor").get<long double>();

			if (usageUnit == "GiBy.mo")
				baseUnitConversionFactor /= SECONDS_PER_MONTH;
			else if (usageUnit == "GiBy.d")
				baseUnitConversionFactor /= SECONDS_PER_DAY;
			
			for (const json& rateJson : pricingJson.at("tieredRates"))
			{
				try
				{
					const std::uint32_t startUsageAmount = rateJson.at("startUsageAmount").get<std::uint32_t>();
					const std::int32_t nanoPrice = rateJson.at("unitPrice").at("nanos").get<std::int32_t>();
					prices.emplace_back(static_cast<std::uint64_t>(startUsageAmount * baseUnitConversionFactor), nanoPrice);
				}
				catch (const json::exception& error)
				{
					std::cout << "Failed to load rate of SKU ID \"" << skuId << "\": ";
					std::cout << error.what() << std::endl;
				}
			}
			
		}
		catch (const json::out_of_range& error)
		{
			std::cout << "Failed to prices of SKU ID \"" << skuId;
			std::cout << "\": " << error.what() << std::endl;
			return prices;
		}
		return prices;
	}


	CCloudFactory::CCloudFactory()
	{
		CCloudFactoryManager::GetRef().AddFactory("gcp", std::unique_ptr<ICloudFactory>(this));
	}
	CCloudFactory::~CCloudFactory()
	{
		mInstance = nullptr;
	}
	auto CCloudFactory::CreateCloud(std::string&& cloudName) const -> std::unique_ptr<IBaseCloud>
	{
		return std::make_unique<CCloud>(std::move(cloudName));
	}
}
