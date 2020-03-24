#pragma once

#include <unordered_map>

#include "CScheduleable.hpp"

#include "common/utils.hpp"

class IPreparedInsert;
class IBaseSim;
class CRucio;
class CBaseTransferManager;
class CStorageElement;


class CDataGenerator : public CScheduleable
{
private:
    IBaseSim* mSim;

    std::unique_ptr<IValueGenerator> mNumFilesGen;
    std::unique_ptr<IValueGenerator> mFileSizeGen;
    std::unique_ptr<IValueGenerator> mFileLifetimeGen;

    TickType mTickFreq;

    auto GetRandomFileSize() const -> SpaceType;
    auto GetRandomNumFilesToGenerate() const -> std::uint32_t;
    auto GetRandomLifeTime() const -> TickType;

    void CreateFilesAndReplicas(const std::uint32_t numFiles, const std::uint32_t numReplicasPerFile, const TickType now);

public:

    bool mSelectStorageElementsRandomly = false;
    std::vector<float> mNumReplicaRatio;
    std::vector<CStorageElement*> mStorageElements;

    CDataGenerator( IBaseSim* sim,
                    std::unique_ptr<IValueGenerator>&& numFilesRNG,
                    std::unique_ptr<IValueGenerator>&& fileSizeRNG,
                    std::unique_ptr<IValueGenerator>&& fileLifetimeRNG,
                    const TickType tickFreq,
                    const TickType startTick=0);

    void CreateFilesAndReplicas(const TickType now);

    void OnUpdate(const TickType now) final;
};



class CReaperCaller : public CScheduleable
{
private:
    CRucio *mRucio;
    TickType mTickFreq;

public:
    CReaperCaller(CRucio *rucio, const TickType tickFreq, const TickType startTick=600);

    void OnUpdate(const TickType now) final;
};



class CBillingGenerator : public CScheduleable
{
private:
    std::shared_ptr<IPreparedInsert> mCloudBillInsertQuery;

    IBaseSim* mSim;
    TickType mTickFreq;

public:
    CBillingGenerator(IBaseSim* sim, const TickType tickFreq = SECONDS_PER_MONTH, const TickType startTick = SECONDS_PER_MONTH);

    void OnUpdate(const TickType now) final;
};



class CHeartbeat : public CScheduleable
{
private:
    IBaseSim* mSim;
    TickType mTickFreq;

    std::chrono::high_resolution_clock::time_point mTimeLastUpdate;

public:
    std::unordered_map<std::string, std::chrono::duration<double>*> mProccessDurations;
    std::vector<std::shared_ptr<CBaseTransferManager>> mTransferManagers;

public:
    CHeartbeat(IBaseSim* sim, const TickType tickFreq, const TickType startTick = 0);

    void OnUpdate(const TickType now) final;
};
