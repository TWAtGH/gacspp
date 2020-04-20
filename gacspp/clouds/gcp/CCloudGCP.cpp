#include <cassert>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "CCloudGCP.hpp"

#include "common/CConfigManager.hpp"

#include "infrastructure/CNetworkLink.hpp"
#include "infrastructure/SFile.hpp"

#include "third_party/nlohmann/json.hpp"


namespace gcp
{
    ICloudFactory* CCloudFactory::mInstance = new CCloudFactory;

    static long double CalculateCostsRecursive(long double amount, TieredPriceType::const_iterator curLevelIt, const TieredPriceType::const_iterator &endIt, std::uint64_t prevThreshold = 0)
    {
        assert(curLevelIt->first >= prevThreshold);

        const std::uint64_t threshold = curLevelIt->first - prevThreshold;
        TieredPriceType::const_iterator nextLevelIt = curLevelIt + 1;

        if (amount <= threshold || nextLevelIt == endIt)
            return (amount * curLevelIt->second) / 1000000000.0;

        const long double lowerLevelCosts = CalculateCostsRecursive(amount - threshold, nextLevelIt, endIt, curLevelIt->first);

        return ((threshold * curLevelIt->second) / 1000000000.0) + lowerLevelCosts;
    }



    CCloudBill::CCloudBill(double storageCost, double networkCost, double traffic, double operationCost, std::size_t numClassA, std::size_t numClassB)
        : mStorageCost(storageCost),
          mNetworkCost(networkCost),
          mTraffic(traffic),
          mOperationCost(operationCost),
          mNumClassA(numClassA),
          mNumClassB(numClassB)
    {}

    std::string CCloudBill::ToString() const
    {
        std::stringstream res;
        res << std::fixed << std::setprecision(2);
        res << std::setw(12) << "Storage: " << mStorageCost << " CHF" << std::endl;
        res << std::setw(12) << "Network: " << mNetworkCost << " CHF (" << mTraffic << " GiB)" << std::endl;
        res << std::setw(12) << "Operations: " << mOperationCost << " CHF ";
        res << "(ClassA: " << mNumClassA / 1000 << "k + ClassB: " << mNumClassB / 1000 << "k)" << std::endl;
        return res.str();
    }


    void CBucket::OnOperation(OPERATION op)
    {
        CStorageElement::OnOperation(op);
        switch(op)
        {
            case INSERT:
                mCostTracking->mNumClassA += 1;
            break;
            case GET:
                mCostTracking->mNumClassB += 1;
            break;
            default: break;
        }
    }

    void CBucket::OnIncreaseReplica(SpaceType amount, TickType now)
    {
        if (now > mTimeLastCostUpdate)
        {
            mCostTracking->mStorageCosts += (BYTES_TO_GiB(mDelegate->GetUsedStorage()) * GetCurStoragePrice() * (now - mTimeLastCostUpdate)) / 1000000000.0;
            mTimeLastCostUpdate = now;
        }
        CStorageElement::OnIncreaseReplica(amount, now);
    }

    void CBucket::RemoveReplica(SReplica* replica, TickType now, bool needLock)
    {
        if (now > mTimeLastCostUpdate)
        {
            mCostTracking->mStorageCosts += (BYTES_TO_GiB(mDelegate->GetUsedStorage()) * GetCurStoragePrice() * (now - mTimeLastCostUpdate)) / 1000000000.0;
            mTimeLastCostUpdate = now;
        }
        CStorageElement::RemoveReplica(replica, now, needLock);
    }

    auto CBucket::CalculateStorageCosts(TickType now) -> double
    {
        if (now > mTimeLastCostUpdate)
        {
            mCostTracking->mStorageCosts += (BYTES_TO_GiB(mDelegate->GetUsedStorage()) * GetCurStoragePrice() * (now - mTimeLastCostUpdate)) / 1000000000.0;
            mTimeLastCostUpdate = now;
        }

        double costs = mCostTracking->mStorageCosts;
        mCostTracking->mStorageCosts = 0;
        return costs;
    }

    auto CBucket::CalculateOperationCosts() -> double
    {
        const TieredPriceType& classAOpPrice = mPriceData->mClassAOpPrice;
        const TieredPriceType& classBOpPrice = mPriceData->mClassBOpPrice;

        assert(!classAOpPrice.empty());
        assert(!classBOpPrice.empty());

        std::size_t numClassA = mCostTracking->mNumClassA;
        std::size_t numClassB = mCostTracking->mNumClassB;

        const double cost = CalculateCostsRecursive(numClassA, classAOpPrice.cbegin(), classAOpPrice.cend())
                            + CalculateCostsRecursive(numClassB, classBOpPrice.cbegin(), classBOpPrice.cend());

        mCostTracking->mNumClassA = mCostTracking->mNumClassA = 0;

        return cost;
    }

    auto CBucket::GetCurStoragePrice() const -> long double
    {
        const TieredPriceType& storagePrice = mPriceData->mStoragePrice;
        assert(!storagePrice.empty());

        std::size_t rateIdx = 0, i = 1;
        while( (i < storagePrice.size()) && (mDelegate->GetUsedStorage() > storagePrice[i].first) )
        {
            ++i;
            ++rateIdx;
        }
        return storagePrice[rateIdx].second;
    }



    auto CRegion::CreateStorageElement(std::string&& name, bool allowDuplicateReplicas, SpaceType quota) -> CBucket*
    {
        mStorageElements.emplace_back(std::make_unique<CBucket>(std::move(name), this, allowDuplicateReplicas, quota));
        return mStorageElements.back().get();
    }

    auto CRegion::GetStorageElements() const -> std::vector<CStorageElement*>
    {
        std::vector<CStorageElement*> storageElements;
        for (const std::unique_ptr<CBucket>& bucket : mStorageElements)
            storageElements.push_back(bucket.get());

        return storageElements;
    }

    double CRegion::CalculateStorageCosts(TickType now)
    {
        double regionStorageCosts = 0;
        for (const std::unique_ptr<CBucket>& bucket : mStorageElements)
            regionStorageCosts += bucket->CalculateStorageCosts(now);
        return regionStorageCosts;
    }

    double CRegion::CalculateOperationCosts(std::size_t& numClassA, std::size_t& numClassB)
    {
        double regionOperationCosts = 0;
        for (const std::unique_ptr<CBucket>& bucket : mStorageElements)
        {
            numClassA += bucket->GetNumClassA();
            numClassB += bucket->GetNumClassB();
            regionOperationCosts += bucket->CalculateOperationCosts();
        }
        return regionOperationCosts;
    }

    double CRegion::CalculateNetworkCosts(double& sumUsedTraffic, std::uint64_t& sumDoneTransfers)
    {
        double regionNetworkCosts = 0;

        for(const std::unique_ptr<CBucket>& srcBucket : mStorageElements)
        {
            for (const std::unique_ptr<CNetworkLink>& networkLink : srcBucket->GetNetworkLinks())
            {
                const TieredPriceType& networkPrice = mNetworkLinkIdToPrice.at(networkLink->GetId());
                const double inGiB = BYTES_TO_GiB(networkLink->mUsedTraffic);
                double costs = CalculateCostsRecursive(inGiB, networkPrice.cbegin(), networkPrice.cend());

                regionNetworkCosts += costs;
                sumUsedTraffic += inGiB;
                sumDoneTransfers += networkLink->mNumDoneTransfers;
                networkLink->mUsedTraffic = 0;
                networkLink->mNumDoneTransfers = 0;
                networkLink->mNumFailedTransfers = 0;
            }
        }
        return regionNetworkCosts;
    }



    CCloud::~CCloud() = default;

    auto CCloud::CreateRegion(  std::string&& name,
                                std::string&& locationName,
                                std::uint8_t multiLocationIdx) -> CRegion*
    {
        auto newRegion = std::make_unique<CRegion>(std::move(name), std::move(locationName), multiLocationIdx);
        CRegion* newRegionRaw = newRegion.get();
        mRegions.emplace_back(std::move(newRegion));
        return newRegionRaw;
    }

    auto CCloud::ProcessBilling(TickType now) -> std::unique_ptr<ICloudBill>
    {
        double totalStorageCosts = 0;
        double totalOperationCosts = 0;
        double totalNetworkCosts = 0;
        double sumUsedTraffic = 0;
        std::size_t numClassA = 0;
        std::size_t numClassB = 0;
        std::uint64_t sumDoneTransfer = 0;
        for (const std::unique_ptr<ISite>& site : mRegions)
        {
            auto region = dynamic_cast<CRegion*>(site.get());
            assert(region != nullptr);
            const double regionStorageCosts = region->CalculateStorageCosts(now);
            const double regionOperationCosts = region->CalculateOperationCosts(numClassA, numClassB);
            const double regionNetworkCosts = region->CalculateNetworkCosts(sumUsedTraffic, sumDoneTransfer);
            totalStorageCosts += regionStorageCosts;
            totalOperationCosts += regionOperationCosts;
            totalNetworkCosts += regionNetworkCosts;
        }
        return std::make_unique<CCloudBill>(totalStorageCosts, totalNetworkCosts, sumUsedTraffic, totalOperationCosts, numClassA, numClassB);
    }

    void CCloud::InitialiseNetworkLinks()
    {
        for (const std::unique_ptr<ISite>& srcSite : mRegions)
        {
            CRegion* srcRegion = dynamic_cast<CRegion*>(srcSite.get());
            assert(srcRegion != nullptr);

            for (const std::unique_ptr<CBucket>& srcBucket : srcRegion->mStorageElements)
            {
                for (const std::unique_ptr<CNetworkLink>& networkLink : srcBucket->GetNetworkLinks())
                {
                    const ISite* dstSite = networkLink->GetDstStorageElement()->GetSite();
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

            mSKUSettings = std::make_unique<json>();
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

            mNetworkPrices = std::make_unique<json>(networkPricesJson);
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

                    for (const auto& [regionJsonKey, regionJsonValue] : regionJson.items())
                    {
                        if (regionJsonKey == "buckets")
                        {
                            assert(regionJsonValue.is_array());
                            for (const json& bucketJson : regionJsonValue)
                            {
                                std::string name, storageSKUId, classAOpSKUId, classsBOpSKUId;
                                try
                                {
                                    bucketJson.at("name").get_to(name);
                                    bucketJson.at("storageSKUId").get_to(storageSKUId);
                                    bucketJson.at("classAOpSKUId").get_to(classAOpSKUId);
                                    bucketJson.at("classBOpSKUId").get_to(classsBOpSKUId);
                                }
                                catch (const json::out_of_range& error)
                                {
                                    std::cout << "Failed getting settings for bucket: " << error.what() << std::endl;
                                    continue;
                                }

                                const SpaceType quota = bucketJson.contains("quota") ? bucketJson["quota"].get<SpaceType>() : 0;
                                const bool duplicates = bucketJson.contains("allowDuplicateReplicas") ? bucketJson["allowDuplicateReplicas"].get<bool>() : false;
                                CBucket* bucket = region->CreateStorageElement(std::move(name), duplicates, quota);

                                bucket->mPriceData->mStoragePrice = GetTieredRateFromSKUId(std::move(storageSKUId));
                                bucket->mPriceData->mClassAOpPrice = GetTieredRateFromSKUId(std::move(classAOpSKUId));
                                bucket->mPriceData->mClassBOpPrice = GetTieredRateFromSKUId(std::move(classsBOpSKUId));
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
            long double baseUnitConversionFactor = 1;
            if (usageUnit == "GiBy.mo")
                baseUnitConversionFactor = pricingJson.at("baseUnitConversionFactor").get<long double>() / ONE_GiB;
            else if (usageUnit == "GiBy.d")
                baseUnitConversionFactor = pricingJson.at("baseUnitConversionFactor").get<long double>() / ONE_GiB;
            else if (usageUnit == "By")
                baseUnitConversionFactor = 1 / ONE_GiB;
            else if(usageUnit == "count" || usageUnit == "GiBy")
                baseUnitConversionFactor = 1;
            else
                std::cout << "Unknown usageUnit: " << usageUnit << std::endl;

            for (const json& rateJson : pricingJson.at("tieredRates"))
            {
                try
                {
                    const std::uint32_t startUsageAmount = rateJson.at("startUsageAmount").get<std::uint32_t>();
                    const std::int32_t price = rateJson.at("unitPrice").at("nanos").get<std::int32_t>();
                    prices.emplace_back(startUsageAmount, price / baseUnitConversionFactor);
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
