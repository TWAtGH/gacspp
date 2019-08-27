#include <cassert>
#include <sstream>

#include "sqlite3.h"

#include "CAdvancedSim.hpp"
#include "CCloudGCP.hpp"
#include "CConfigLoader.hpp"
#include "CLinkSelector.hpp"
#include "CRucio.hpp"
#include "COutput.hpp"
#include "CommonScheduleables.hpp"



void CAdvancedSim::SetupDefaults()
{
    COutput& output = COutput::GetRef();
    CConfigLoader& config = CConfigLoader::GetRef();
    ////////////////////////////
    // init output db
    ////////////////////////////
    std::stringstream dbIn;
    bool ok = false;

    ok = output.CreateTable("Sites", "id BIGINT PRIMARY KEY, name varchar(64), locationName varchar(32), cloudName varchar(32)");
    assert(ok);

    ok = output.CreateTable("StorageElements", "id BIGINT PRIMARY KEY, siteId BIGINT, name varchar(64), FOREIGN KEY(siteId) REFERENCES Sites(id)");
    assert(ok);

    ok = output.CreateTable("LinkSelectors", "id BIGINT PRIMARY KEY, srcSiteId BIGINT, dstSiteId BIGINT, FOREIGN KEY(srcSiteId) REFERENCES Sites(id), FOREIGN KEY(dstSiteId) REFERENCES Sites(id)");
    assert(ok);

    ok = output.CreateTable("Files", "id BIGINT PRIMARY KEY, createdAt BIGINT, expiredAt BIGINT, filesize INTEGER");
    assert(ok);

    dbIn.str(std::string());
    dbIn << "id BIGINT PRIMARY KEY,"
         << "fileId BIGINT,"
         << "storageElementId BIGINT,"
         << "createdAt BIGINT,"
         << "expiredAt BIGINT,"
         << "FOREIGN KEY(fileId) REFERENCES Files(id),"
         << "FOREIGN KEY(storageElementId) REFERENCES StorageElements(id)";
    ok = output.CreateTable("Replicas", dbIn.str());
    assert(ok);

    dbIn.str(std::string());
    dbIn << "id BIGINT PRIMARY KEY,"
         << "srcReplicaId BIGINT,"
         << "dstReplicaId BIGINT,"
         << "startTick BIGINT,"
         << "endTick BIGINT,"
         << "FOREIGN KEY(srcReplicaId) REFERENCES Replicas(id),"
         << "FOREIGN KEY(dstReplicaId) REFERENCES Replicas(id)";
    ok = output.CreateTable("Transfers", dbIn.str());
    assert(ok);

    //CStorageElement::mOutputQueryIdx = output.AddPreparedSQLStatement("INSERT INTO Replicas VALUES(?, ?, ?, ?, ?);");
    CStorageElement::outputReplicaInsertQuery = output.CreatePreparedInsert("INSERT INTO Replicas VALUES(?, ?, ?, ?, ?);", '?');


    ////////////////////////////
    // setup grid and clouds
    ////////////////////////////
    mRucio = std::make_unique<CRucio>();
    mClouds.emplace_back(std::make_unique<gcp::CCloud>("GCP"));

    config.mConfigConsumer.push_back(mRucio.get());
    for(std::unique_ptr<IBaseCloud>& cloud : mClouds)
        config.mConfigConsumer.push_back(cloud.get());

    config.TryLoadConfig(std::filesystem::current_path() / "config" / "default.json");

    //add all grid sites and storage elements to output DB (before links)
    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
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
                 << "'" << storageElement->GetName() << "'";
            ok = ok && output.InsertRow("StorageElements", dbIn.str());
        }
    }

    //add all cloud regions and buckets to output DB and then create and add all links
    for(const std::unique_ptr<IBaseCloud>& cloud : mClouds)
    {
        for(const std::unique_ptr<ISite>& cloudSite : cloud->mRegions)
        {
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
                     << "'" << bucket->GetName() << "'";
                ok = ok && output.InsertRow("StorageElements", dbIn.str());
            }

            for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
            {
                CLinkSelector* link = gridSite->CreateLinkSelector(region, ONE_GiB / 32);
                dbIn.str(std::string());
                dbIn << link->GetId() << ","
                     << link->GetSrcSiteId() << ","
                     << link->GetDstSiteId();
                ok = ok && output.InsertRow("LinkSelectors", dbIn.str());

                link = region->CreateLinkSelector(gridSite.get(), ONE_GiB / 128);
                dbIn.str(std::string());
                dbIn << link->GetId() << ","
                     << link->GetSrcSiteId() << ","
                     << link->GetDstSiteId();
                ok = ok && output.InsertRow("LinkSelectors", dbIn.str());
            }
        }
    }

    for(const std::unique_ptr<IBaseCloud>& cloud : mClouds)
        cloud->SetupDefaultCloud();

    assert(ok);

    ////////////////////////////
    // setup scheuleables
    ////////////////////////////
    auto dataGen = std::make_shared<CDataGenerator>(this, 50, 0);
    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
        for(const std::unique_ptr<CStorageElement>& gridStoragleElement : gridSite->mStorageElements)
            dataGen->mStorageElements.push_back(gridStoragleElement.get());

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
            CJobSlotTransferGen::SJobSlotInfo jobslot = {region->mNumJobSlots, {}};
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
