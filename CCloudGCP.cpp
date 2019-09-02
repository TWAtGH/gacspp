#include <cassert>
#include <iomanip>
#include <iostream>

#include "json.hpp"

#include "CCloudGCP.hpp"
#include "CLinkSelector.hpp"
#include "SFile.hpp"



namespace gcp
{
    CBucket::CBucket(std::string&& name, CRegion* region)
        : CStorageElement(std::move(name), region),
          mRegion(region)
    {}

    void CBucket::OnOperation(const CStorageElement::OPERATION op)
    {
        CStorageElement::OnOperation(op);
        switch(op)
        {
            case CStorageElement::INSERT:
                mOperationCosts += 0.05;
            break;
            case CStorageElement::GET:
                mOperationCosts += 0.004;
            break;
            default: break;
        }
    }

    void CBucket::OnIncreaseReplica(std::uint64_t amount, TickType now)
    {
        if (now > mTimeLastCostUpdate)
        {
            mStorageCosts += BYTES_TO_GiB(mUsedStorage) * mRegion->GetStoragePrice() * SECONDS_TO_MONTHS((now - mTimeLastCostUpdate));
            mTimeLastCostUpdate = now;
        }
        CStorageElement::OnIncreaseReplica(amount, now);
    }

    void CBucket::OnRemoveReplica(const SReplica* replica, TickType now)
    {
        std::unique_lock<std::mutex> lock(mReplicaRemoveMutex);
        if (now > mTimeLastCostUpdate)
        {
            mStorageCosts += BYTES_TO_GiB(mUsedStorage) * mRegion->GetStoragePrice() * SECONDS_TO_MONTHS((now - mTimeLastCostUpdate));
            mTimeLastCostUpdate = now;
        }
        CStorageElement::OnRemoveReplica(replica, now, false);
    }

    double CBucket::CalculateStorageCosts(TickType now)
    {
        if (now > mTimeLastCostUpdate)
        {
            mStorageCosts += BYTES_TO_GiB(mUsedStorage) * mRegion->GetStoragePrice() * SECONDS_TO_MONTHS((now - mTimeLastCostUpdate));
            mTimeLastCostUpdate = now;
        }

        double costs = mStorageCosts;
        mStorageCosts = 0;
        return costs;
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

    CRegion::CRegion(std::string&& name, std::string&& locationName, const std::uint32_t multiLocationIdx, const double storagePrice, std::string&& skuId)
	    : ISite(std::move(name), std::move(locationName)),
	      mSKUId(std::move(skuId)),
	      mStoragePrice(storagePrice)
    {}

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



    auto CCloud::CreateRegion(const std::uint32_t multiLocationIdx,
                                std::string&& name,
                                std::string&& locationName,
                                const std::uint32_t numJobSlots,
                                const double storagePrice,
                                std::string&& skuId) -> CRegion*
    {
	    CRegion* newRegion = new CRegion(multiLocationIdx, std::move(name), std::move(locationName), numJobSlots, storagePrice, std::move(skuId));
	    mRegions.emplace_back(newRegion);
	    return newRegion;
    }

    auto CCloud::ProcessBilling(TickType now) -> std::pair<double, std::pair<double, double>>
    {
	    double totalStorageCosts = 0;
	    double totalNetworkCosts = 0;
        double sumUsedTraffic = 0;
        std::uint64_t sumDoneTransfer = 0;
	    for (const std::unique_ptr<ISite>& site : mRegions)
	    {
		    auto region = dynamic_cast<CRegion*>(site.get());
		    assert(region != nullptr);
		    const double regionStorageCosts = region->CalculateStorageCosts(now);
		    const double regionNetworkCosts = region->CalculateNetworkCosts(sumUsedTraffic, sumDoneTransfer);
		    totalStorageCosts += regionStorageCosts;
		    totalNetworkCosts += regionNetworkCosts;
	    }
        return { totalStorageCosts, {totalNetworkCosts, sumUsedTraffic } };
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

	    typedef std::unordered_map<std::string, CLinkSelector::PriceInfoType> InnerMapType;
	    typedef std::unordered_map<std::string, InnerMapType> OuterMapType;

	    const OuterMapType priceWW
	    {
		    { "0", {{ "0", priceSameMulti },
                    { "1",{ { 1024, 0.1775835 }, { 10240, 0.1682370 }, { 10240, 0.1401975 } } },
				    { "2",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
				    { "3",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
				    { "4",{ {1, 0.0}, { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
				 }
		    },
		    { "1", {{ "1", priceSameMulti },
                    { "2",{ { 1024, 0.1775835 },{ 10240, 0.1682370 },{ 10240, 0.1401975 } } },
				    { "3",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
				    { "4",{ { 1024, 0.1775835 },{ 10240, 0.1682370 },{ 10240, 0.1401975 } } }
				 }
		    },
		    { "2", {{ "2", priceSameMulti },
                    { "3",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } },
				    { "4",{ { 1, 0.0 },{ 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
				 }
		    },
		    { "3", {{ "3", priceSameMulti },
                    { "4",{ { 1024, 0.1121580 },{ 10240, 0.1028115 },{ 10240, 0.0747720 } } }
                 }
            },
            { "4", {{ "4", priceSameMulti } } }
	    };

	    for (const std::unique_ptr<ISite>& srcSite : mRegions)
	    {
		    auto srcRegion = dynamic_cast<CRegion*>(srcSite.get());
		    assert(srcRegion != nullptr);

            const std::string srcRegionMultiLocation = srcRegion->mCustomConfig["multiLocation"];
            assert(srcRegionMultiLocation.empty() == false);

            //first set prices for existing links (outgoing links)
            for(std::unique_ptr<CLinkSelector> &linkSelector : srcRegion->mLinkSelectors)
            {
                OuterMapType::const_iterator outerIt = priceWW.find(srcRegionMultiLocation);
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
                    innerIt = outerIt->second.find(srcRegionMultiLocation);
                }

                assert(innerIt != outerIt->second.cend());
                linkSelector->mNetworkPrice = innerIt->second;
            }

		    for (const std::unique_ptr<ISite>& dstSite : mRegions)
		    {
			    auto dstRegion = dynamic_cast<CRegion*>(dstSite.get());
			    assert(dstRegion != nullptr);
                const std::string dstRegionMultiLocation = dstRegion->mCustomConfig["multiLocation"];
                assert(dstRegionMultiLocation.empty() == false);
			    const bool isSameLocation = (*srcRegion) == (*dstRegion);
			    if (!isSameLocation && (srcRegionMultiLocation != dstRegionMultiLocation))
			    {
				    OuterMapType::const_iterator outerIt = priceWW.find(srcRegionMultiLocation);
				    InnerMapType::const_iterator innerIt;
				    if(outerIt != priceWW.cend())
				    {
					    //found the srcRegion idx in the outer map
					    //lets see if we find the dstRegion idx in the inner map
					    innerIt = outerIt->second.find(dstRegionMultiLocation);
					    if(innerIt == outerIt->second.cend())
						    outerIt = priceWW.cend(); //Nope. Reset outerIt and try with swapped order
				    }
				    if(outerIt == priceWW.cend())
				    {
					    outerIt = priceWW.find(dstRegionMultiLocation);
					    assert(outerIt != priceWW.cend());
					    innerIt = outerIt->second.find(srcRegionMultiLocation);
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

    bool CCloud::TryConsumeConfig(const nlohmann::json& json)
    {
        nlohmann::json::const_iterator rootIt = json.find("gcp");
        if(rootIt == json.cend())
            return false;
        for( const auto& [key, value] : rootIt.value().items() )
        {
            if( key == "regions" )
            {
                for(const auto& regionJson : value)
                {
                    std::string regionName, regionLocation, skuId;
                    double price = 0;
                    nlohmann::json bucketsJson;
                    std::unordered_map<std::string, std::string> customConfig;
                    for(const auto& [regionJsonKey, regionJsonValue] : regionJson.items())
                    {
                        if(regionJsonKey == "name")
                            regionName = regionJsonValue.get<std::string>();
                        else if(regionJsonKey == "location")
                            regionLocation = regionJsonValue.get<std::string>();
                        else if(regionJsonKey == "buckets")
                            bucketsJson = regionJsonValue;
                        else if(regionJsonKey == "price")
                            price = regionJsonValue.get<double>();
                        else if(regionJsonKey == "skuId")
                            skuId = regionJsonValue.get<std::string>();
                        else if(regionJsonValue.type() == json::value_t::string)
                            customConfig[regionJsonKey.get<std::string>()] = regionJsonValue.get<std::string>();
                        else
                            customConfig[regionJsonKey.get<std::string>()] = regionJsonValue.dump();
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
            	    CRegion *region = CreateRegion(*multiLocationIdx, std::move(regionName), std::move(regionLocation), numJobSlots, price, std::move(skuId));
                    region->mCustomConfig = std::move(customConfig);

                    if (bucketsJson.empty())
                    {
                        std::cout << "No buckets to create for this region" << std::endl;
                        continue;
                    }

                    for(const auto& bucketJson : bucketsJson)
                    {
                        std::string bucketName;
                        for(const auto& [bucketJsonKey, bucketJsonValue] : bucketJson.items())
                        {
                            if(bucketJsonKey == "name")
                                bucketName = bucketJsonValue.get<std::string>();
                            else
                                std::cout << "Ignoring unknown attribute while loading bucket: " << bucketJsonKey << std::endl;
                        }

                        if (bucketName.empty())
                        {
                            std::cout << "Couldn't find name attribute of bucket" << std::endl;
                            continue;
                        }

                        std::cout << "Adding bucket " << bucketName << std::endl;
                        region->CreateStorageElement(std::move(bucketName));
                    }
                }
            }
        }
        return true;
    }
}
