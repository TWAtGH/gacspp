#pragma once

#include <chrono>
#include <unordered_map>

#include "constants.h"
#include "CScheduleable.hpp"

class IBaseSim;
class CRucio;
class CStorageElement;
class CLinkSelector;
struct SReplica;



class CDataGenerator : public CScheduleable
{
private:
    std::shared_ptr<IPreparedInsert> mOutputFileInsertQuery;

    IBaseSim* mSim;

    std::normal_distribution<float> mNumFilesRNG {40, 1};
    std::normal_distribution<double> mFileSizeRNG {0.5, 0.25};
    std::normal_distribution<float> mFileLifetimeRNG {6, 1};

    std::uint32_t mTickFreq;

    std::uint32_t GetRandomFileSize();
    std::uint32_t GetRandomNumFilesToGenerate();
    TickType GetRandomLifeTime();

    std::uint64_t CreateFilesAndReplicas(const std::uint32_t numFiles, const std::uint32_t numReplicasPerFile, const TickType now);

public:
    std::vector<CStorageElement*> mStorageElements;
    CDataGenerator(IBaseSim* sim, const std::uint32_t tickFreq, const TickType startTick=0);

    void OnUpdate(const TickType now) final;
};


class CReaper : public CScheduleable
{
private:
    CRucio *mRucio;
    std::uint32_t mTickFreq;

public:
    CReaper(CRucio *rucio, const std::uint32_t tickFreq, const TickType startTick=600);

    void OnUpdate(const TickType now) final;
};

class CBillingGenerator : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::uint32_t mTickFreq;

public:
    CBillingGenerator(IBaseSim* sim, const std::uint32_t tickFreq=SECONDS_PER_MONTH, const TickType startTick=SECONDS_PER_MONTH);

    void OnUpdate(const TickType now) final;
};


class CTransferManager : public CScheduleable
{
private:
    std::shared_ptr<IPreparedInsert> mOutputTransferInsertQuery;

    TickType mLastUpdated = 0;
    std::uint32_t mTickFreq;

    struct STransfer
    {
        std::weak_ptr<SReplica> mSrcReplica;
        std::weak_ptr<SReplica> mDstReplica;
        CLinkSelector* mLinkSelector;
        TickType mStartTick;

        STransfer(  std::shared_ptr<SReplica> srcReplica,
                    std::shared_ptr<SReplica> dstReplica,
                    CLinkSelector* const linkSelector,
                    const TickType startTick);
    };

    std::vector<STransfer> mActiveTransfers;

public:
    std::uint32_t mNumCompletedTransfers = 0;
    TickType mSummedTransferDuration = 0;

public:
    CTransferManager(const std::uint32_t tickFreq, const TickType startTick=0);

    void OnUpdate(const TickType now) final;

    void CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now);

    inline auto GetNumActiveTransfers() const -> std::size_t
    {return mActiveTransfers.size();}
};


class CFixedTimeTransferManager : public CScheduleable
{
private:
    std::shared_ptr<IPreparedInsert> mOutputTransferInsertQuery;

    TickType mLastUpdated = 0;
    std::uint32_t mTickFreq;

    struct STransfer
    {
        std::weak_ptr<SReplica> mSrcReplica;
        std::weak_ptr<SReplica> mDstReplica;
        CLinkSelector* mLinkSelector;
        TickType mStartTick;

        std::uint32_t mIncreasePerTick;

        STransfer(  std::shared_ptr<SReplica> srcReplica,
                    std::shared_ptr<SReplica> dstReplica,
                    CLinkSelector* const linkSelector,
                    const TickType startTick,
                    const std::uint32_t increasePerTick);
    };

    std::vector<STransfer> mActiveTransfers;

public:
    std::uint32_t mNumCompletedTransfers = 0;
    TickType mSummedTransferDuration = 0;

public:
    CFixedTimeTransferManager(const std::uint32_t tickFreq, const TickType startTick=0);

    void OnUpdate(const TickType now) final;

    void CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now, const TickType duration);

    inline auto GetNumActiveTransfers() const -> std::size_t
    {return mActiveTransfers.size();}
};


class CBaseTransferNumGen
{
public:
    virtual auto GetNumToCreate(RNGEngineType& rngEngine, std::uint32_t numActive, const TickType now) -> std::uint32_t = 0;
};


class CWavedTransferNumGen : public CBaseTransferNumGen
{
public:
    double mSoftmaxScale;
    double mSoftmaxOffset;
    double mAlpha = 1.0/30.0 * PI/180.0 * 0.075;

    std::normal_distribution<double> mSoftmaxRNG {0, 1};
    std::normal_distribution<double> mPeakinessRNG {1.05, 0.04};

public:
    CWavedTransferNumGen(const double softmaxScale, const double softmaxOffset, const std::uint32_t samplingFreq, const double baseFreq);

    auto GetNumToCreate(RNGEngineType& rngEngine, std::uint32_t numActive, const TickType now) -> std::uint32_t;
};


class CUniformTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    std::uint32_t mTickFreq;

public:
    std::shared_ptr<CBaseTransferNumGen> mTransferNumGen;
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

public:
    CUniformTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        std::shared_ptr<CBaseTransferNumGen> transferNumGen,
                        const std::uint32_t tickFreq,
                        const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
};


class CExponentialTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    std::uint32_t mTickFreq;

public:
    std::shared_ptr<CBaseTransferNumGen> mTransferNumGen;
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

public:
    CExponentialTransferGen(IBaseSim* sim,
                            std::shared_ptr<CTransferManager> transferMgr,
                            std::shared_ptr<CBaseTransferNumGen> transferNumGen,
                            const std::uint32_t tickFreq,
                            const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
};


class CSrcPrioTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    std::uint32_t mTickFreq;

public:
    std::shared_ptr<CBaseTransferNumGen> mTransferNumGen;
    std::unordered_map<IdType, int> mSrcStorageElementIdToPrio;
    std::vector<CStorageElement*> mDstStorageElements;

public:
    CSrcPrioTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        std::shared_ptr<CBaseTransferNumGen> transferNumGen,
                        const std::uint32_t tickFreq,
                        const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
};


class CJobSlotTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CFixedTimeTransferManager> mTransferMgr;
    std::uint32_t mTickFreq;

public:

    struct SJobSlotInfo
    {
        std::uint32_t mNumMaxSlots;
        std::vector<std::pair<TickType, std::uint32_t>> mSchedule;
    };

    std::unordered_map<IdType, int> mSrcStorageElementIdToPrio;
    std::vector<std::pair<CStorageElement*, SJobSlotInfo>> mDstInfo;

public:
    CJobSlotTransferGen(IBaseSim* sim,
                        std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                        const std::uint32_t tickFreq,
                        const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
};


class CHeartbeat : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CFixedTimeTransferManager> mG2CTransferMgr;
    std::shared_ptr<CTransferManager> mC2CTransferMgr;
    std::uint32_t mTickFreq;

    std::chrono::high_resolution_clock::time_point mTimeLastUpdate;

public:
    std::unordered_map<std::string, std::chrono::duration<double>*> mProccessDurations;

public:
    CHeartbeat(IBaseSim* sim, std::shared_ptr<CFixedTimeTransferManager> g2cTransferMgr, std::shared_ptr<CTransferManager> c2cTransferMgr, const std::uint32_t tickFreq, const TickType startTick=0);

    void OnUpdate(const TickType now) final;
};
