#include <iostream>

#include "CTestSim.hpp"

#include "CommonScheduleables.hpp"

#include "third_party/nlohmann/json.hpp"



bool CTestSim::SetupDefaults(const json& profileJson)
{
    if(!CDefaultBaseSim::SetupDefaults(profileJson))
        return false;

    ////////////////////////////
    // setup scheuleables
    ////////////////////////////
    std::unordered_map<std::string, CStorageElement*> nameToStorageElement;
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
            std::cout << "Failed to load cached transfer gen cfg: only fixed transfer implemented" << std::endl;
            return false;
        }
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load cached transfer gen cfg: " << error.what() << std::endl;
        return false;
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

    return true;
}
