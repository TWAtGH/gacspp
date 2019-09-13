#include <cassert>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "json.hpp"

#include "CCloudGCP.hpp"
#include "CLinkSelector.hpp"
#include "SFile.hpp"



namespace gcp
{
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
        std::size_t rateIdx = 0;
        while( (rateIdx < mStorageRates.size()) && (mUsedStorage > mStorageRates[rateIdx].first) )
            ++rateIdx;
        return static_cast<double>(mStorageRates[rateIdx].second) / 1000000000.0;
    }



    static double CalculateNetworkCostsRecursive(std::uint64_t traffic, CLinkSelector::PriceInfoType::const_iterator curLevelIt, const CLinkSelector::PriceInfoType::const_iterator &endIt, std::uint64_t prevThreshold = 0)
    {
	    assert(curLevelIt->first >= prevThreshold);
	    const std::uint64_t threshold = curLevelIt->first - prevThreshold;
	    CLinkSelector::PriceInfoType::const_iterator nextLevelIt = curLevelIt + 1;
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
	    for (const std::unique_ptr<CLinkSelector>& linkSelector : mLinkSelectors)
	    {
            double costs = CalculateNetworkCostsRecursive(linkSelector->mUsedTraffic, linkSelector->mNetworkPrice.cbegin(), linkSelector->mNetworkPrice.cend());

            regionNetworkCosts += costs;
            sumUsedTraffic += BYTES_TO_GiB(linkSelector->mUsedTraffic);
            sumDoneTransfers += linkSelector->mDoneTransfers;
		    linkSelector->mUsedTraffic = 0;
            linkSelector->mDoneTransfers = 0;
            linkSelector->mFailedTransfers = 0;
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
	    //CreateRegion("us", "northamerica-northeast1", "Montreal", 0.02275045, "E466-8D73-08F4");
	    //CreateRegion("northamerica-northeast1', 'northamerica-northeast1', 'Montreal', 0.02275045, "E466-8D73-08F4")

	    /*
	    eu - apac EF0A-B3BA-32CA 0.1121580 0.1121580 0.1028115 0.0747720
	    na - apac 6B37-399C-BF69 0.0000000 0.1121580 0.1028115 0.0747720
	    na - eu   C7FF-4F9E-C0DB 0.0000000 0.1121580 0.1028115 0.0747720

	    au - apac CDD1-6B91-FDF8 0.1775835 0.1775835 0.1682370 0.1401975
	    au - eu   1E7D-CBB0-AF0C 0.1775835 0.1775835 0.1682370 0.1401975
	    au - na   27F0-D54C-619A 0.1775835 0.1775835 0.1682370 0.1401975
	    au - sa   7F66-C883-4D7D 0.1121580 0.1121580 0.1028115 0.0747720
	    apac - sa 1F9A-A9AC-FFC3 0.1121580 0.1121580 0.1028115 0.0747720
	    eu - sa   96EB-C6ED-FBDE 0.1121580 0.1121580 0.1028115 0.0747720
	    na - sa   BB86-91E8-5450 0.1121580 0.1121580 0.1028115 0.0747720
	    */

	    //download apac      1F8B-71B0-3D1B 0.0000000 0.1121580 0.1028115 0.0747720
	    //download australia 9B2D-2B7D-FA5C 0.1775835 0.1775835 0.1682370 0.1401975
	    //download china     4980-950B-BDA6 0.2149695 0.2149695 0.2056230 0.1869300
	    //download us emea   22EB-AAE8-FBCD 0.0000000 0.1121580 0.1028115 0.0747720


	    const CLinkSelector::PriceInfoType priceSameRegion = { {0,0} };
	    const CLinkSelector::PriceInfoType priceSameMulti = { {1,0.0093465} };

	    typedef std::unordered_map<std::uint8_t, CLinkSelector::PriceInfoType> InnerMapType;
	    typedef std::unordered_map<std::uint8_t, InnerMapType> OuterMapType;

	    const OuterMapType priceWW
	    {
		    { 0, {  { 0, priceSameMulti },
                    { 1,{ { 1024, 0.1775835 }, { 10240, 0.1682370 }, { 10240, 0.1401975 } } },
				    { 2,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
				    { 3,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
				    { 4,{ {1, 0.0}, { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
				 }
		    },
		    { 1, {  { 1, priceSameMulti },
                    { 2,{ { 1024, 0.1775835 },{ 10240, 0.1682370 },{ 10240, 0.1401975 } } },
				    { 3,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
				    { 4,{ { 1024, 0.1775835 },{ 10240, 0.1682370 },{ 10240, 0.1401975 } } }
				 }
		    },
		    { 2, {  { 2, priceSameMulti },
                    { 3,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
				    { 4,{ { 1, 0.0 },{ 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
				 }
		    },
		    { 3, {  { 3, priceSameMulti },
                    { 4,{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
                 }
            },
            { 4, {  { 4, priceSameMulti } } }
	    };

	    for (const std::unique_ptr<ISite>& srcSite : mRegions)
	    {
		    auto srcRegion = dynamic_cast<CRegion*>(srcSite.get());
		    assert(srcRegion != nullptr);

            const std::uint8_t srcRegionMultiLocationIdx = srcRegion->GetMultiLocationIdx();

            //first set prices for existing links (outgoing links)
            for(std::unique_ptr<CLinkSelector> &linkSelector : srcRegion->mLinkSelectors)
            {
                OuterMapType::const_iterator outerIt = priceWW.find(srcRegionMultiLocationIdx);
                InnerMapType::const_iterator innerIt;
                if(outerIt != priceWW.cend())
                {
                    //found the srcRegion idx in the outer map
                    //lets see if we find the dstRegion idx in the inner map
                    innerIt = outerIt->second.find(linkSelector->GetDstSite()->GetMultiLocationIdx());
                    if(innerIt == outerIt->second.cend())
                        outerIt = priceWW.cend(); //Nope. Reset outerIt and try in swapped order
                }
                if(outerIt == priceWW.cend())
                {
                    outerIt = priceWW.find(linkSelector->GetDstSite()->GetMultiLocationIdx());
                    assert(outerIt != priceWW.cend());
                    innerIt = outerIt->second.find(srcRegionMultiLocationIdx);
                }

                assert(innerIt != outerIt->second.cend());
                linkSelector->mNetworkPrice = innerIt->second;
            }

		    for (const std::unique_ptr<ISite>& dstSite : mRegions)
		    {
			    auto dstRegion = dynamic_cast<CRegion*>(dstSite.get());
			    assert(dstRegion != nullptr);
                const std::uint8_t dstRegionMultiLocationIdx = dstRegion->GetMultiLocationIdx();
			    const bool isSameLocation = (*srcRegion) == (*dstRegion);
			    if (!isSameLocation && (srcRegionMultiLocationIdx != dstRegionMultiLocationIdx))
			    {
				    OuterMapType::const_iterator outerIt = priceWW.find(srcRegionMultiLocationIdx);
				    InnerMapType::const_iterator innerIt;
				    if(outerIt != priceWW.cend())
				    {
					    //found the srcRegion idx in the outer map
					    //lets see if we find the dstRegion idx in the inner map
					    innerIt = outerIt->second.find(dstRegionMultiLocationIdx);
					    if(innerIt == outerIt->second.cend())
						    outerIt = priceWW.cend(); //Nope. Reset outerIt and try with swapped order
				    }
				    if(outerIt == priceWW.cend())
				    {
					    outerIt = priceWW.find(dstRegionMultiLocationIdx);
					    assert(outerIt != priceWW.cend());
					    innerIt = outerIt->second.find(srcRegionMultiLocationIdx);
				    }

				    assert(innerIt != outerIt->second.cend());
				    CLinkSelector* linkSelector = srcRegion->CreateLinkSelector(dstRegion, ONE_GiB/64);
				    linkSelector->mNetworkPrice = innerIt->second;
			    }
			    else if (isSameLocation)
			    {
				    // 2. case: r1 and r2 are the same region
				    //linkselector.network_price_chf = priceSameRegion
				    CLinkSelector* linkSelector = srcRegion->CreateLinkSelector(dstRegion, ONE_GiB/8);
				    linkSelector->mNetworkPrice = priceSameRegion;
			    }
			    else
			    {
				    // 3. case: region r1 is inside the multi region r2
				    //linkselector.network_price_chf = priceSameMulti
				    CLinkSelector* linkSelector = srcRegion->CreateLinkSelector(dstRegion, ONE_GiB/32);
				    linkSelector->mNetworkPrice = priceSameMulti;
			    }
		    }
	    }
    }

    bool CCloud::TryConsumeConfig(const json& config)
    {
        json::const_iterator rootIt = config.find("gcp");
        if(rootIt == config.cend())
            return false;

        json gcpConfJson = rootIt.value();

        {
            json::const_iterator baseSettingsFilesPropIt = gcpConfJson.find("baseSettingsFiles");
            if(baseSettingsFilesPropIt != gcpConfJson.cend())
            {
                for(const json& baseSettingsFileJson : baseSettingsFilesPropIt.value())
                {
                    std::filesystem::path baseSettingsFilePath = std::filesystem::current_path() / "config" / baseSettingsFileJson.get<std::string>();
                    std::ifstream baseSettingsFile(baseSettingsFilePath.string());
                    if(baseSettingsFile)
                    {
                        json baseSettingsJson;
                        baseSettingsFile >> baseSettingsJson;
                        LoadBaseSettingsJson(baseSettingsJson);
                    }
                    else
                        std::cout << "Unable to load base settings file: " << baseSettingsFilePath.string() << std::endl;
                }
            }
        }

        for( const auto& [key, value] : gcpConfJson.items() )
        {
            if( key == "regions" )
            {
                for(const auto& regionJson : value)
                {
                    std::unique_ptr<std::uint8_t> multiLocationIdx;
                    std::string regionName, regionLocation;
                    json bucketsJson;
                    std::unordered_map<std::string, std::string> customConfig;
                    for(const auto& [regionJsonKey, regionJsonValue] : regionJson.items())
                    {
                        if(regionJsonKey == "name")
                            regionName = regionJsonValue.get<std::string>();
                        else if(regionJsonKey == "location")
                            regionLocation = regionJsonValue.get<std::string>();
                        else if(regionJsonKey == "multiLocationIdx")
                            multiLocationIdx = std::make_unique<std::uint8_t>(regionJsonValue.get<std::uint8_t>());
                        else if(regionJsonKey == "buckets")
                            bucketsJson = regionJsonValue;
                        else if(regionJsonValue.type() == json::value_t::string)
                            customConfig[regionJsonKey] = regionJsonValue.get<std::string>();
                        else
                            customConfig[regionJsonKey] = regionJsonValue.dump();
                    }

                    if (multiLocationIdx == nullptr)
                    {
                        std::cout << "Couldn't find multiLocationIdx attribute of region" << std::endl;
                        continue;
                    }

                    if (regionName.empty())
                    {
                        std::cout << "Couldn't find name attribute of region" << std::endl;
                        continue;
                    }

                    if (regionLocation.empty())
                    {
                        std::cout << "Couldn't find location attribute of region: " << regionName << std::endl;
                        continue;
                    }

                    std::cout << "Adding region " << regionName << " in " << regionLocation << std::endl;
            	    CRegion *region = CreateRegion(std::move(regionName), std::move(regionLocation), *multiLocationIdx);
                    region->mCustomConfig = std::move(customConfig);

                    if (bucketsJson.empty())
                    {
                        std::cout << "No buckets to create for this region" << std::endl;
                        continue;
                    }

                    for(const auto& bucketJson : bucketsJson)
                    {
                        std::string bucketName, skuId;
                        for(const auto& [bucketJsonKey, bucketJsonValue] : bucketJson.items())
                        {
                            if(bucketJsonKey == "name")
                                bucketName = bucketJsonValue.get<std::string>();
                            else if(bucketJsonKey == "skuId")
                                skuId = bucketJsonValue.get<std::string>();
                            else
                                std::cout << "Ignoring unknown attribute while loading bucket: " << bucketJsonKey << std::endl;
                        }

                        if (bucketName.empty())
                        {
                            std::cout << "Couldn't find name attribute of bucket" << std::endl;
                            continue;
                        }

                        if (skuId.empty())
                        {
                            std::cout << "Couldn't find skuId attribute of bucket" << std::endl;
                            continue;
                        }

                        std::cout << "Adding bucket " << bucketName << std::endl;
                        CBucket* bucket = region->CreateStorageElement(std::move(bucketName));
                        InitBucketBySKUId(bucket, skuId);
                    }
                }
            }
        }
        return true;
    }

    void CCloud::LoadBaseSettingsJson(const json& config)
    {
        json::const_iterator skuPropIt = config.find("skus");
        if(skuPropIt == config.cend())
        {
            std::cout<<"Cannot load base settings"<<std::endl;
            return;
        }
        mSKUSettings = new json;
        for(const auto& skuJson : skuPropIt.value())
        {
            json::const_iterator skuIdPropIt = skuJson.find("skuId");
            if(skuIdPropIt == skuJson.cend())
            {
                std::cout<<"Ignoring object without SKU ID"<<std::endl;
                continue;
            }
            std::string skuId = skuIdPropIt.value().get<std::string>();
            if(mSKUSettings->count(skuId) > 0)
            {
                std::cout<<"Ignoring second object for same SKU ID: "<<skuId<<std::endl;
                continue;
            }
            (*mSKUSettings)[skuId] = skuJson;
        }
    }

    void CCloud::InitBucketBySKUId(CBucket* bucket, const std::string& skuId)
    {
        //todo: remove this func and use CreateStorageElement() with a factory object
        const json* prop = mSKUSettings;
        json::const_iterator propIt;

        //<skuID> -> pricingInfo -> pricingExpression -> tieredRates: startUsageAmount; unitPrice -> nanos
        std::vector<std::string> keys({skuId, "pricingInfo", "pricingExpression", "tieredRates"});

        for(const std::string& propName : keys)
        {
            while(prop->is_array())
                prop = &((*prop)[0]);

            propIt = prop->find(propName);
            if(propIt == prop->cend())
            {
                std::cout<<"Couldn't find property \""<<propName;
                std::cout<<"\" for SKU ID \""<<skuId;
                std::cout<<"\" while setting up bucket: "<<bucket->GetName()<<std::endl;
                return;
            }
            prop = &(propIt.value());
        }

        for(const json& rateJson : (*prop))
        {
            std::uint32_t startUsageAmount = 0;
            std::int32_t nanoPrice = 0;
            propIt = rateJson.find("startUsageAmount");
            if(propIt != rateJson.cend())
                startUsageAmount = propIt.value().get<std::uint32_t>();
            else
                std::cout<<"Couldn't find prop startUsageAmount for SKU ID: "<<skuId<<std::endl;

            propIt = rateJson.find("unitPrice");
            if(propIt != rateJson.cend())
            {
                const json& unitPriceJson = propIt.value();
                propIt = unitPriceJson.find("nanos");
                if(propIt != unitPriceJson.cend())
                    nanoPrice = propIt.value().get<std::int32_t>();
                else
                    std::cout<<"Couldn't find prop nanos for SKU ID: "<<skuId<<std::endl;
            }
            else
                std::cout<<"Couldn't find prop unitPrice for SKU ID: "<<skuId<<std::endl;
            bucket->mStorageRates.emplace_back(startUsageAmount, nanoPrice);
        }
    }
}
