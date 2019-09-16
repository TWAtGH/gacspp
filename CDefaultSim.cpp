#include <cassert>
#include <iostream>
#include <sstream>

#include "json.hpp"

#include "CDefaultSim.hpp"
#include "CCloudGCP.hpp"
#include "CConfigManager.hpp"
#include "CNetworkLink.hpp"
#include "CRucio.hpp"
#include "COutput.hpp"
#include "CommonScheduleables.hpp"



void CDefaultSim::SetupDefaults(const json& profileJson)
{
	CConfigManager& configManager = CConfigManager::GetRef();
    COutput& output = COutput::GetRef();

    //CStorageElement::outputReplicaInsertQuery = output.CreatePreparedInsert("INSERT INTO Replicas VALUES(?, ?, ?, ?, ?)", '?');
    CStorageElement::outputReplicaInsertQuery = output.CreatePreparedInsert("COPY Replicas(id, fileId, storageElementId, createdAt, expiredAt) FROM STDIN with(FORMAT csv);", 5, '?');


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
    auto dataGen = std::make_shared<CDataGenerator>(this, 50, 0);
    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
        for(const std::unique_ptr<CStorageElement>& gridStoragleElement : gridSite->mStorageElements)
            dataGen->mStorageElements.push_back(gridStoragleElement.get());
    dataGen->mStorageElements.push_back(((gcp::CRegion*)(mClouds[0]->mRegions[0].get()))->mStorageElements[0].get());

    auto reaper = std::make_shared<CReaper>(mRucio.get(), 600, 600);

    auto x2cTransferMgr = std::make_shared<CFixedTimeTransferManager>(20, 100);
    //auto x2cTransferNumGen = std::make_shared<CWavedTransferNumGen>(12, 200, 25, 0.075);
    //auto x2cTransferGen = std::make_shared<CSrcPrioTransferGen>(this, x2cTransferMgr, x2cTransferNumGen, 25);
    auto x2cTransferGen = std::make_shared<CJobSlotTransferGen>(this, x2cTransferMgr, 25);


    auto heartbeat = std::make_shared<CHeartbeat>(this, x2cTransferMgr, nullptr, static_cast<std::uint32_t>(SECONDS_PER_DAY), static_cast<TickType>(SECONDS_PER_DAY));
    heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["X2CTransferUpdate"] = &(x2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["X2CTransferGen"] = &(x2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);


    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
        for(const std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
        {
            x2cTransferGen->mSrcStorageElementIdToPrio[storageElement->GetId()] = 0;
        }
    }

    for(const std::unique_ptr<ISite>& cloudSite : mClouds[0]->mRegions)
    {
        auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
        assert(region);
        for (const std::unique_ptr<gcp::CBucket>& bucket : region->mStorageElements)
        {
            x2cTransferGen->mSrcStorageElementIdToPrio[bucket->GetId()] = 1;
            //x2cTransferGen->mDstStorageElements.push_back(bucket.get());
            CJobSlotTransferGen::SJobSlotInfo jobslot = {static_cast<std::uint32_t>(std::stoi(region->mCustomConfig["numJobSlots"])), {}};
            x2cTransferGen->mDstInfo.push_back( std::make_pair(bucket.get(), jobslot) );
        }
    }

    mSchedule.push(std::make_shared<CBillingGenerator>(this));
    mSchedule.push(dataGen);
    mSchedule.push(reaper);
    mSchedule.push(x2cTransferMgr);
    mSchedule.push(x2cTransferGen);
    mSchedule.push(heartbeat);
}
