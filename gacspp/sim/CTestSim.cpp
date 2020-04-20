#include <iostream>

#include "CTestSim.hpp"

#include "clouds/IBaseCloud.hpp"

#include "infrastructure/CRucio.hpp"
#include "infrastructure/CStorageElement.hpp"

#include "scheduleables/CommonScheduleables.hpp"
#include "scheduleables/TransferManager.hpp"
#include "scheduleables/TransferGenerators.hpp"

#include "output/IDatabase.hpp"

#include "third_party/nlohmann/json.hpp"



bool CTestSim::SetupDefaults(const json& profileJson)
{
    if(!CDefaultBaseSim::SetupDefaults(profileJson))
        return false;

    ////////////////////////////
    // setup scheuleables
    ////////////////////////////

    if(!profileJson.contains("transferCfgs"))
    {
        std::cout << "No transfer configuration" << std::endl;
        return false;
    }


    auto heartbeat = std::make_shared<CHeartbeat>(this, static_cast<std::uint32_t>(SECONDS_PER_DAY), static_cast<TickType>(SECONDS_PER_DAY));
    heartbeat->mName = "Heartbeat";

    for (const json& transferCfg : profileJson.at("transferCfgs"))
    {
        json transferManagerCfg, transferGenCfg;
        try
        {
            transferManagerCfg = transferCfg.at("manager");
            transferGenCfg = transferCfg.at("generator");
        }
        catch (const json::out_of_range& error)
        {
            std::cout << "Invalid transfer configuration: " << error.what() << std::endl;
            return false;
        }

        auto transferManager = std::dynamic_pointer_cast<CTransferManager>(CreateTransferManager(transferManagerCfg));
        if (!transferManager)
        {
            std::cout << "Failed creating transfer manager" << std::endl;
            return false;
        }
        heartbeat->mProccessDurations.push_back(transferManager);

        auto transferGen = std::dynamic_pointer_cast<CCloudBufferTransferGen>(CreateTransferGenerator(transferGenCfg, transferManager));
        if (!transferGen)
        {
            auto transferGen2 = std::dynamic_pointer_cast<CJobIOTransferGen>(CreateTransferGenerator(transferGenCfg, transferManager));
            if(!transferGen2)
            {
                std::cout << "Failed creating transfer generator" << std::endl;
                return false;
            }
            heartbeat->mProccessDurations.push_back(transferGen2);
            mSchedule.push(transferGen2);
        }
        else
        {
            heartbeat->mProccessDurations.push_back(transferGen);
            mSchedule.push(transferGen);
        }

        heartbeat->mTransferManagers.push_back(transferManager);

        mSchedule.push(transferManager);
    }

    std::vector<CStorageElement*> storageElements;
    for (const std::unique_ptr<CGridSite>& site : mRucio->mGridSites)
    {
        std::vector<CStorageElement*> s = site->GetStorageElements();
        storageElements.insert(storageElements.end(), s.begin(), s.end());
    }

    for (const std::unique_ptr<IBaseCloud>& cloud : mClouds)
    {
        for (const std::unique_ptr<ISite>& region : cloud->mRegions)
        {
            std::vector<CStorageElement*> s = region->GetStorageElements();
            storageElements.insert(storageElements.end(), s.begin(), s.end());
        }
    }

    mDeletionInserter = std::make_shared<CBufferedOnDeletionInsert>();
    for (CStorageElement* storageElement : storageElements)
        storageElement->mActionListener.emplace_back(mDeletionInserter.get());
    mRucio->mActionListener.emplace_back(mDeletionInserter.get());

    try
    {
        for(const json& dataGenCfg : profileJson.at("dataGens"))
        {
            TickType tickFreq = 0;
            TickType startTick = 0;
            if(dataGenCfg.contains("tickFreq"))
                tickFreq = dataGenCfg["tickFreq"].get<TickType>();
            if(dataGenCfg.contains("startTick"))
                tickFreq = dataGenCfg["startTick"].get<TickType>();

            std::unordered_map<std::string, std::unique_ptr<IValueGenerator>> jsonPropNameToValueGen;

            if(!dataGenCfg.contains("numFilesCfg"))
                jsonPropNameToValueGen["numFilesCfg"] = std::make_unique<CFixedValueGenerator>(0);

            for(const std::string& propName : {"numFilesCfg", "fileSizeCfg", "lifetimeCfg"})
            {
                if(jsonPropNameToValueGen.count(propName) > 0)
                    continue;

                const json& propJson = dataGenCfg.at(propName);
                jsonPropNameToValueGen[propName] = IValueGenerator::CreateFromJson(propJson);
            }

            std::shared_ptr<CDataGenerator> dataGen = std::make_shared<CDataGenerator>(this,
                                                                            std::move(jsonPropNameToValueGen["numFilesCfg"]),
                                                                            std::move(jsonPropNameToValueGen["fileSizeCfg"]),
                                                                            std::move(jsonPropNameToValueGen["lifetimeCfg"]),
                                                                            tickFreq, startTick);
            dataGen->mName = dataGenCfg.at("name").get<std::string>();
            for(const json& storageElementJson : dataGenCfg.at("storageElements"))
            {
                CStorageElement* storageElement = GetStorageElementByName(storageElementJson.get<std::string>());
                if(!storageElement)
                {
                    std::cout<<"Failed to find storage element for data generator: "<<storageElementJson.get<std::string>()<<std::endl;
                    continue;
                }
                dataGen->mStorageElements.push_back(storageElement);
            }

            for(const json& ratioVal : dataGenCfg.at("numReplicaRatios"))
                dataGen->mNumReplicaRatio.push_back(ratioVal.get<float>());

            dataGen->mSelectStorageElementsRandomly = dataGenCfg.at("selectStorageElementsRandomly").get<bool>();

            heartbeat->mProccessDurations.push_back(dataGen);

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
        reaper->mName = reaperCfg.at("name").get<std::string>();

        heartbeat->mProccessDurations.push_back(reaper);
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load reaper cfg: " << error.what() << std::endl;
        reaper = std::make_shared<CReaperCaller>(mRucio.get(), 600, 600);
        reaper->mName = "DefaultReaper";
    }

    auto billGen = std::make_shared<CBillingGenerator>(this);
    billGen->mName = "BillingGenerator";
    mSchedule.push(billGen);
    //mSchedule.push(reaper);
    mSchedule.push(heartbeat);

    return true;
}
