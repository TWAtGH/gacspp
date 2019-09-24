#include <cassert>
#include <iostream>
#include <sstream>

#include "CDefaultSim.hpp"
#include "CommonScheduleables.hpp"

#include "clouds/gcp/CCloudGCP.hpp"

#include "common/CConfigManager.hpp"

#include "infrastructure/CNetworkLink.hpp"
#include "infrastructure/CRucio.hpp"

#include "output/COutput.hpp"

#include "third_party/json.hpp"



void CDefaultSim::SetupDefaults(const json& profileJson)
{
    CConfigManager& configManager = CConfigManager::GetRef();
    COutput& output = COutput::GetRef();


    ////////////////////////////
    // setup grid and clouds
    ////////////////////////////
    mRucio = std::make_unique<CRucio>();
    try
    {
        json rucioCfg;
        configManager.TryLoadProfileCfg(rucioCfg, configManager.GetFileNameFromObj(profileJson.at("rucio")));
        mRucio->LoadConfig(rucioCfg);
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load rucio cfg: " << error.what() << std::endl;
    }

    try
    {
        for (const json& cloudJson : profileJson.at("clouds"))
        {
            std::unique_ptr<IBaseCloud> cloud;
            try
            {
                const std::string cloudId = cloudJson.at("id").get<std::string>();
                cloud = CCloudFactoryManager::GetRef().CreateCloud(cloudId, cloudJson.at("name").get<std::string>());

                if (!cloud)
                {
                    std::cout << "Failed to create cloud with id: " << cloudId << std::endl;
                    continue;
                }
            }
            catch (const json::exception& error)
            {
                std::cout << "Failed to load config for cloud: " << error.what() << std::endl;
                continue;
            }

            json cloudCfg;
            configManager.TryLoadProfileCfg(cloudCfg, configManager.GetFileNameFromObj(cloudJson));
            cloud->LoadConfig(cloudCfg);
            mClouds.emplace_back(std::move(cloud));
        }
    }
    catch (const json::out_of_range& error)
    {
        std::cout << "Failed to load clouds cfg: " << error.what() << std::endl;
    }

    std::unordered_map<std::string, ISite*> nameToSite;
    std::unordered_map<std::string, CStorageElement*> nameToStorageElement;
    std::stringstream dbIn;
    bool ok = true;
    //add all grid sites and storage elements to output DB (before links)
    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
        assert(nameToSite.count(gridSite->GetName()) == 0);
        nameToSite[gridSite->GetName()] = gridSite.get();

        dbIn.str(std::string());
        dbIn << gridSite->GetId() << ","
             << "'" << gridSite->GetName() << "',"
             << "'" << gridSite->GetLocationName() << "',"
             << "'grid'";
        ok = ok && output.InsertRow("Sites", dbIn.str());

        for(const std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
        {
            assert(nameToStorageElement.count(storageElement->GetName()) == 0);
            nameToStorageElement[storageElement->GetName()] = storageElement.get();

            dbIn.str(std::string());
            dbIn << storageElement->GetId() << ","
                 << gridSite->GetId() << ","
                 << "'" << storageElement->GetName() << "',"
                 << storageElement->GetAccessLatency();
            ok = ok && output.InsertRow("StorageElements", dbIn.str());
        }
    }

    //add all cloud regions and buckets to output DB and then create and add all links
    for(const std::unique_ptr<IBaseCloud>& cloud : mClouds)
    {
        for(const std::unique_ptr<ISite>& cloudSite : cloud->mRegions)
        {
            assert(nameToSite.count(cloudSite->GetName()) == 0);
            nameToSite[cloudSite->GetName()] = cloudSite.get();

            auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
            dbIn.str(std::string());
            dbIn << region->GetId() << ","
                 << "'" << region->GetName() << "',"
                 << "'" << region->GetLocationName() << "',"
                 << "'" << cloud->GetName() << "'";
            ok = ok && output.InsertRow("Sites", dbIn.str());

            for(const std::unique_ptr<gcp::CBucket>& bucket : region->mStorageElements)
            {
                assert(nameToStorageElement.count(bucket->GetName()) == 0);
                nameToStorageElement[bucket->GetName()] = bucket.get();

                dbIn.str(std::string());
                dbIn << bucket->GetId() << ","
                     << region->GetId() << ","
                     << "'" << bucket->GetName() << "',"
                     << bucket->GetAccessLatency();
                ok = ok && output.InsertRow("StorageElements", dbIn.str());
            }
        }
    }

    try
    {
        json linksCfg;
        configManager.TryLoadProfileCfg(linksCfg, configManager.GetFileNameFromObj(profileJson.at("links")));

        for(const auto& [srcSiteName, dstNameCfgJson] : linksCfg.items())
        {
            if(nameToSite.count(srcSiteName) == 0)
            {
                std::cout<<"Failed to find src site for link configuration: "<<srcSiteName<<std::endl;
                continue;
            }

            ISite* srcSite = nameToSite[srcSiteName];
            for(const auto& [dstSiteName, dstLinkCfgJson] : dstNameCfgJson.items())
            {
                if(nameToSite.count(dstSiteName) == 0)
                {
                    std::cout<<"Failed to find dst site for link configuration: "<<dstSiteName<<std::endl;
                    continue;
                }
                ISite* dstSite = nameToSite[dstSiteName];
                try
                {
                    std::uint32_t bandwidth = dstLinkCfgJson.at("bandwidth").get<std::uint32_t>();
                    CNetworkLink* link = srcSite->CreateNetworkLink(dstSite, bandwidth);
                    dbIn.str(std::string());
                    dbIn << link->GetId() << ","
                         << link->GetSrcSiteId() << ","
                         << link->GetDstSiteId();
                    ok = ok && output.InsertRow("NetworkLinks", dbIn.str());

                    if(dstLinkCfgJson.at("receivingLink").empty())
                        continue;

                    bandwidth = dstLinkCfgJson.at("receivingLink").at("bandwidth").get<std::uint32_t>();
                    link = dstSite->CreateNetworkLink(srcSite, bandwidth);
                    dbIn.str(std::string());
                    dbIn << link->GetId() << ","
                         << link->GetSrcSiteId() << ","
                         << link->GetDstSiteId();
                    ok = ok && output.InsertRow("NetworkLinks", dbIn.str());
                }
                catch (const json::out_of_range& error)
                {
                    std::cout << "Failed to create link: " << error.what() << std::endl;
                }
            }
        }
    }
    catch (const json::out_of_range& error)
    {
        std::cout << "Failed to load links cfg: " << error.what() << std::endl;
    }

    assert(ok);

    for(const std::unique_ptr<IBaseCloud>& cloud : mClouds)
        cloud->InitialiseNetworkLinks();


    ////////////////////////////
    // setup scheuleables
    ////////////////////////////
    std::shared_ptr<CCachedSrcTransferGen> transferGen;
    std::shared_ptr<CFixedTimeTransferManager> manager;
    try
    {
        const json& transferGenCfg = profileJson.at("cachedTransferGen");
        const std::string managerType = transferGenCfg.at("managerType").get<std::string>();
        const TickType tickFreq = transferGenCfg.at("tickFreq").get<TickType>();
        const TickType startTick = transferGenCfg.at("startTick").get<TickType>();
        const TickType managerTickFreq = transferGenCfg.at("managerTickFreq").get<TickType>();
        const TickType managerStartTick = transferGenCfg.at("managerStartTick").get<TickType>();
        const std::size_t numPerDay = transferGenCfg.at("numPerDay").get<std::size_t>();
        const TickType defaultReplicaLifetime = transferGenCfg.at("defaultReplicaLifetime").get<TickType>();
        if(managerType == "fixedTime")
        {
            manager = std::make_shared<CFixedTimeTransferManager>(managerTickFreq, managerStartTick);
            transferGen = std::make_shared<CCachedSrcTransferGen>(this, manager, numPerDay, defaultReplicaLifetime, tickFreq, startTick);

            for(const json& srcStorageElementName : transferGenCfg.at("srcStorageElements"))
                transferGen->mSrcStorageElements.push_back( nameToStorageElement[srcStorageElementName.get<std::string>()] );
            for(const json& cacheStorageElementJson : transferGenCfg.at("cacheStorageElements"))
            {
                const std::size_t cacheSize = cacheStorageElementJson.at("size").get<std::size_t>();
                const TickType defaultReplicaLifetime = cacheStorageElementJson.at("defaultReplicaLifetime").get<TickType>();
                CStorageElement* storageElement = nameToStorageElement[cacheStorageElementJson.at("storageElement").get<std::string>()];
                transferGen->mCacheElements.push_back( {cacheSize, defaultReplicaLifetime, storageElement} );
            }
            for(const json& dstStorageElementName : transferGenCfg.at("dstStorageElements"))
                transferGen->mDstStorageElements.push_back( nameToStorageElement[dstStorageElementName.get<std::string>()] );
        }
        else
        {
            std::cout << "Failed to load reaper cached transfer gen cfg: only fixed transfer implemented" << std::endl;
        }
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load reaper cached transfer gen cfg: " << error.what() << std::endl;
    }

    mRucio->mFileActionListeners.emplace_back(transferGen);
    mRucio->mReplicaActionListeners.emplace_back(transferGen);

    try
    {
        for(const json& dataGenCfg : profileJson.at("dataGens"))
        {
            const TickType tickFreq = dataGenCfg.at("tickFreq").get<TickType>();
            const TickType startTick = dataGenCfg.at("startTick").get<TickType>();

            std::unordered_map<std::string, std::unique_ptr<IValueGenerator>> jsonPropNameToValueGen;
            for(const std::string& propName : {"numFilesCfg", "fileSizeCfg", "lifetimeCfg"})
            {
                const json& propJson = dataGenCfg.at(propName);
                const std::string type = propJson.at("type").get<std::string>();
                if(type == "normal")
                {
                    const double mean = propJson.at("mean");
                    const double stddev = propJson.at("stddev");
                    jsonPropNameToValueGen[propName] = std::make_unique<CNormalRandomValueGenerator>(mean, stddev);
                }
                else if(type == "fixed")
                {
                    const double value = propJson.at("value");
                    jsonPropNameToValueGen[propName] = std::make_unique<CFixedValueGenerator>(value);
                }
            }

            std::shared_ptr<CDataGenerator> dataGen = std::make_shared<CDataGenerator>(this,
                                                                            std::move(jsonPropNameToValueGen["numFilesCfg"]),
                                                                            std::move(jsonPropNameToValueGen["fileSizeCfg"]),
                                                                            std::move(jsonPropNameToValueGen["lifetimeCfg"]),
                                                                            tickFreq, startTick);

            for(const json& storageElement : dataGenCfg.at("storageElements"))
            {
                const std::string storageElementName = storageElement.get<std::string>();
                if(nameToStorageElement.count(storageElementName) == 0)
                {
                    std::cout<<"Failed to find storage element for data generator: "<<storageElementName<<std::endl;
                    continue;
                }
                dataGen->mStorageElements.push_back(nameToStorageElement[storageElementName]);
            }

            for(const json& ratioVal : dataGenCfg.at("numReplicaRatios"))
                dataGen->mNumReplicaRatio.push_back(ratioVal.get<float>());

            dataGen->mSelectStorageElementsRandomly = dataGenCfg.at("selectStorageElementsRandomly").get<bool>();

            if(dataGenCfg.contains("numPreSimStartFiles"))
                dataGen->CreateFilesAndReplicas(dataGenCfg["numPreSimStartFiles"].get<std::uint32_t>(), 1, 0);

            mSchedule.push(dataGen);
        }
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load data gen cfg: " << error.what() << std::endl;
    }


    std::shared_ptr<CReaperCaller> reaper;
    try
    {
        const json& reaperCfg = profileJson.at("reaper");
        const TickType tickFreq = reaperCfg.at("tickFreq").get<TickType>();
        const TickType startTick = reaperCfg.at("startTick").get<TickType>();
        reaper = std::make_shared<CReaperCaller>(mRucio.get(), tickFreq, startTick);
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load reaper cfg: " << error.what() << std::endl;
        reaper = std::make_shared<CReaperCaller>(mRucio.get(), 600, 600);
    }

    auto heartbeat = std::make_shared<CHeartbeat>(this, manager, nullptr, static_cast<std::uint32_t>(SECONDS_PER_DAY), static_cast<TickType>(SECONDS_PER_DAY));
    //heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["TransferUpdate"] = &(manager->mUpdateDurationSummed);
    heartbeat->mProccessDurations["TransferGen"] = &(transferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);


    mSchedule.push(std::make_shared<CBillingGenerator>(this));
    mSchedule.push(reaper);
    mSchedule.push(manager);
    mSchedule.push(transferGen);
    mSchedule.push(heartbeat);
}