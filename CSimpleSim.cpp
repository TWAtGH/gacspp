#include <cassert>
#include <sstream>

#include "sqlite3.h"

#include "CCloudGCP.hpp"
#include "CConfigLoader.hpp"
#include "CLinkSelector.hpp"
#include "CRucio.hpp"
#include "CSimpleSim.hpp"
#include "COutput.hpp"
#include "CommonScheduleables.hpp"



void CSimpleSim::SetupDefaults()
{
    COutput& output = COutput::GetRef();
    CConfigLoader& config = CConfigLoader::GetRef();
    ////////////////////////////
    // init output db
    ////////////////////////////
    std::stringstream columns;
    bool ok = false;

    ok = output.CreateTable("Sites", "id BIGINT PRIMARY KEY, name varchar(64), locationName varchar(64)");
    assert(ok);

    ok = output.CreateTable("StorageElements", "id BIGINT PRIMARY KEY, siteId BIGINT, name varchar(64), FOREIGN KEY(siteId) REFERENCES Sites(id)");
    assert(ok);

    ok = output.CreateTable("LinkSelectors", "id BIGINT PRIMARY KEY, srcSiteId BIGINT, dstSiteId BIGINT, FOREIGN KEY(srcSiteId) REFERENCES Sites(id), FOREIGN KEY(dstSiteId) REFERENCES Sites(id)");
    assert(ok);

    ok = output.CreateTable("Files", "id BIGINT PRIMARY KEY, createdAt BIGINT, lifetime BIGINT, filesize INTEGER");
    assert(ok);

    ok = output.CreateTable("Replicas", "fileId BIGINT, storageElementId BIGINT, PRIMARY KEY(fileId, storageElementId)");
    assert(ok);

    columns << "id BIGINT PRIMARY KEY,"
            << "fileId BIGINT,"
            << "srcStorageElementId BIGINT,"
            << "dstStorageElementId BIGINT,"
            << "startTick BIGINT,"
            << "endTick BIGINT,"
            << "FOREIGN KEY(fileId) REFERENCES Files(id),"
            << "FOREIGN KEY(srcStorageElementId) REFERENCES StorageElements(id),"
            << "FOREIGN KEY(dstStorageElementId) REFERENCES StorageElements(id)";
    ok = output.CreateTable("Transfers", columns.str());
    assert(ok);

    //CStorageElement::mOutputQueryIdx = output.AddPreparedSQLStatement("INSERT INTO Replicas VALUES(?, ?);");
    CStorageElement::outputReplicaInsertQuery = output.CreatePreparedInsert("INSERT INTO Replicas VALUES(?, ?);", 2, '?');


    ////////////////////////////
    // setup grid and clouds
    ////////////////////////////
    mRucio = std::make_unique<CRucio>();
    mClouds.emplace_back(std::make_unique<gcp::CCloud>("GCP"));

    config.mConfigConsumer.push_back(mRucio.get());
    for(std::unique_ptr<IBaseCloud>& cloud : mClouds)
        config.mConfigConsumer.push_back(cloud.get());

    config.TryLoadConfig(std::filesystem::current_path() / "config" / "default.json");

    for(const std::unique_ptr<IBaseCloud>& cloud : mClouds)
    {
        cloud->SetupDefaultCloud();
        for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
        {
            for(const std::unique_ptr<ISite>& cloudSite : cloud->mRegions)
            {
                auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
                gridSite->CreateLinkSelector(region, ONE_GiB / 32);
                region->CreateLinkSelector(gridSite.get(), ONE_GiB / 128);
            }
        }
    }


    ////////////////////////////
    // setup scheuleables
    ////////////////////////////
    auto dataGen = std::make_shared<CDataGenerator>(this, 50, 0);

    auto reaper = std::make_shared<CReaper>(mRucio.get(), 600, 600);

    auto g2cTransferMgr = std::make_shared<CTransferManager>(20, 100);
    auto g2cTransferNumGen = std::make_shared<CWavedTransferNumGen>(15, 500, 25, 0.075);
    auto g2cTransferGen = std::make_shared<CExponentialTransferGen>(this, g2cTransferMgr, g2cTransferNumGen, 25);

    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
        for(const std::unique_ptr<CStorageElement>& gridStoragleElement : gridSite->mStorageElements)
        {
            dataGen->mStorageElements.push_back(gridStoragleElement.get());
            g2cTransferGen->mSrcStorageElements.push_back(gridStoragleElement.get());
        }
    }

    auto c2cTransferMgr = std::make_shared<CTransferManager>(20, 100);
    auto c2cTransferNumGen = std::make_shared<CWavedTransferNumGen>(10, 40, 25, 0.075);
    auto c2cTransferGen = std::make_shared<CExponentialTransferGen>(this, c2cTransferMgr, c2cTransferNumGen, 25);

    //auto heartbeat = std::make_shared<CHeartbeat>(this, g2cTransferMgr, c2cTransferMgr, static_cast<std::uint32_t>(SECONDS_PER_DAY), static_cast<TickType>(SECONDS_PER_DAY));
    auto heartbeat = std::make_shared<CHeartbeat>(this, nullptr, c2cTransferMgr, static_cast<std::uint32_t>(SECONDS_PER_DAY), static_cast<TickType>(SECONDS_PER_DAY));
    heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferUpdate"] = &(g2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["G2CTransferGen"] = &(g2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2CTransferUpdate"] = &(c2cTransferMgr->mUpdateDurationSummed);
    heartbeat->mProccessDurations["C2CTransferGen"] = &(c2cTransferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);

    for(const std::unique_ptr<ISite>& cloudSite : mClouds[0]->mRegions)
    {
        auto region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
        assert(region);
        for (const std::unique_ptr<gcp::CBucket>& bucket : region->mStorageElements)
        {
            g2cTransferGen->mDstStorageElements.push_back(bucket.get());
            c2cTransferGen->mSrcStorageElements.push_back(bucket.get());
            c2cTransferGen->mDstStorageElements.push_back(bucket.get());
        }
    }

    mSchedule.push(std::make_shared<CBillingGenerator>(this));
    mSchedule.push(dataGen);
    mSchedule.push(reaper);
    mSchedule.push(g2cTransferMgr);
    mSchedule.push(g2cTransferGen);
    mSchedule.push(c2cTransferMgr);
    mSchedule.push(c2cTransferGen);
    mSchedule.push(heartbeat);
}
