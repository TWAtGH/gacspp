#pragma once

#include <chrono>
#include <unordered_map>

#include "CScheduleable.hpp"

#include "infrastructure/CRucio.hpp"

#include "common/constants.h"



class IBaseSim;
class CRucio;
class CStorageElement;
class CNetworkLink;
struct SFile;
struct SReplica;
class IPreparedInsert;

class CScopedTimeDiff
{
private:
    std::chrono::high_resolution_clock::time_point mStartTime;
    std::chrono::duration<double>& mOutVal;
    bool mWillAdd;

public:
    CScopedTimeDiff(std::chrono::duration<double>& outVal, bool willAdd=false);
    ~CScopedTimeDiff();
};

class IValueGenerator
{
public:
    virtual auto GetValue(RNGEngineType& rngEngine) -> double = 0;
};

class CFixedValueGenerator : public IValueGenerator
{
private:
    double mValue;

public:
    CFixedValueGenerator(const double value);
    virtual auto GetValue(RNGEngineType& rngEngine) -> double;
};

class CNormalRandomValueGenerator : public IValueGenerator
{
private:
    std::normal_distribution<double> mNormalRNGDistribution;

public:
    CNormalRandomValueGenerator(const double mean, const double stddev);
    virtual auto GetValue(RNGEngineType& rngEngine) -> double;
};

class CDataGenerator : public CScheduleable
{
private:
    IBaseSim* mSim;

    std::unique_ptr<IValueGenerator> mNumFilesRNG;
    std::unique_ptr<IValueGenerator> mFileSizeRNG;
    std::unique_ptr<IValueGenerator> mFileLifetimeRNG;

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



class CBaseTransferManager : public CScheduleable
{
public:
    using CScheduleable::CScheduleable;

    std::uint32_t mNumCompletedTransfers = 0;
    std::uint32_t mNumFailedTransfers = 0;
    TickType mSummedTransferDuration = 0;

    virtual auto GetNumActiveTransfers() const -> std::size_t = 0;
};



class CTransferBatchManager : public CBaseTransferManager
{
public:
    struct STransfer
    {
        std::shared_ptr<SReplica> mSrcReplica;
        std::shared_ptr<SReplica> mDstReplica;

        TickType mQueuedAt;
        TickType mStartAt;

        std::size_t mCurRouteIdx = 0;

        STransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, TickType queuedAt, TickType startedAt, std::size_t routeIdx = 0);
    };

    struct STransferBatch
    {
        std::vector<CNetworkLink*> mRoute;

        TickType mStartAt;

        std::vector<std::unique_ptr<STransfer>> mTransfers;
        std::uint32_t mNumDoneTransfers = 0;
    };

public:
    CTransferBatchManager(const TickType tickFreq, const TickType startTick=0);

    void OnUpdate(const TickType now) final;

    void QueueTransferBatch(std::shared_ptr<STransferBatch> transferBatch)
    {
        if(!transferBatch->mTransfers.empty())
            mQueuedTransferBatches.emplace_back(transferBatch);
    }

    auto GetNumActiveTransfers() const -> std::size_t
    {
        return mActiveTransferBatches.size();
    }

private:
    std::shared_ptr<IPreparedInsert> mOutputTransferInsertQuery;

    TickType mLastUpdated = 0;
    TickType mTickFreq;

    std::vector<std::shared_ptr<STransferBatch>> mActiveTransferBatches;
    std::vector<std::shared_ptr<STransferBatch>> mQueuedTransferBatches;

};


class CTransferManager : public CBaseTransferManager
{
private:
    std::shared_ptr<IPreparedInsert> mOutputTransferInsertQuery;

    TickType mLastUpdated = 0;
    TickType mTickFreq;

    struct STransfer
    {
        std::weak_ptr<SReplica> mSrcReplica;
        std::weak_ptr<SReplica> mDstReplica;
        CNetworkLink* mNetworkLink;
        TickType mQueuedAt;
        TickType mStartAt;

        STransfer(  std::shared_ptr<SReplica> srcReplica,
                    std::shared_ptr<SReplica> dstReplica,
                    CNetworkLink* const networkLink,
                    const TickType queuedAt,
                    const TickType startAt);
    };

    std::vector<STransfer> mActiveTransfers;
    std::vector<STransfer> mQueuedTransfers;

public:
    CTransferManager(const TickType tickFreq, const TickType startTick=0);

    void OnUpdate(const TickType now) final;

    void CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now);

    auto GetNumActiveTransfers() const -> std::size_t
    {
        return mActiveTransfers.size();
    }
};


class CFixedTimeTransferManager : public CBaseTransferManager
{
private:
    std::shared_ptr<IPreparedInsert> mOutputTransferInsertQuery;

    TickType mLastUpdated = 0;
    TickType mTickFreq;

    struct STransfer
    {
        std::weak_ptr<SReplica> mSrcReplica;
        std::weak_ptr<SReplica> mDstReplica;
        CNetworkLink* mNetworkLink;
        TickType mQueuedAt;
        TickType mStartAt;

        SpaceType mIncreasePerTick;

        STransfer(  std::shared_ptr<SReplica> srcReplica,
                    std::shared_ptr<SReplica> dstReplica,
                    CNetworkLink* const networkLink,
                    const TickType queuedAt,
                    const TickType startAt,
                    const SpaceType increasePerTick);
    };

    std::vector<STransfer> mActiveTransfers;
    std::vector<STransfer> mQueuedTransfers;

public:
    CFixedTimeTransferManager(const TickType tickFreq, const TickType startTick=0);

    void OnUpdate(const TickType now) final;

    void CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now, const TickType startDelay, const TickType duration);
    void Shutdown(const TickType now) final;


    inline auto GetNumQueuedTransfers() const -> std::size_t
    {return mQueuedTransfers.size();}
    auto GetNumActiveTransfers() const -> std::size_t
    {
        return mActiveTransfers.size();
    }
};


class CWavedTransferNumGen
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


class CBaseOnDeletionInsert : public IFileActionListener, public IReplicaActionListener
{
private:
    std::shared_ptr<IPreparedInsert> mFileInsertQuery;
    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

public:
    CBaseOnDeletionInsert();

    void OnFileCreated(const TickType now, std::shared_ptr<SFile> file) override;
    void OnFilesDeleted(const TickType now, const std::vector<std::weak_ptr<SFile>>& deletedFiles) override;
    void OnReplicaCreated(const TickType now, std::shared_ptr<SReplica> replica) override;
    void OnReplicasDeleted(const TickType now, const std::vector<std::weak_ptr<SReplica>>& deletedReplicas) override;
};


class CSimpleTransferGen : public CScheduleable, public CBaseOnDeletionInsert
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    TickType mTickFreq;

public:
    struct STransferGenInfo
    {
        std::size_t mMaxNumActive;
        CNetworkLink* mNetworkLink;
        std::vector<std::shared_ptr<SFile>> mFiles;
    };
    std::vector<std::unique_ptr<STransferGenInfo>> mTransferGenInfo;

public:
    CSimpleTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        const TickType tickFreq,
                        const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
};


class CUniformTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    TickType mTickFreq;

    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

public:
    std::shared_ptr<CWavedTransferNumGen> mTransferNumGen;
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

public:
    CUniformTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        std::shared_ptr<CWavedTransferNumGen> transferNumGen,
                        const TickType tickFreq,
                        const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
};


class CExponentialTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    TickType mTickFreq;

    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

public:
    std::shared_ptr<CWavedTransferNumGen> mTransferNumGen;
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

public:
    CExponentialTransferGen(IBaseSim* sim,
                            std::shared_ptr<CTransferManager> transferMgr,
                            std::shared_ptr<CWavedTransferNumGen> transferNumGen,
                            const TickType tickFreq,
                            const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
};


class CSrcPrioTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    TickType mTickFreq;

    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

public:
    std::shared_ptr<CWavedTransferNumGen> mTransferNumGen;
    std::unordered_map<IdType, int> mSrcStorageElementIdToPrio;
    std::vector<CStorageElement*> mDstStorageElements;

public:
    CSrcPrioTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        std::shared_ptr<CWavedTransferNumGen> transferNumGen,
                        const TickType tickFreq,
                        const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
};


class CJobSlotTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CFixedTimeTransferManager> mTransferMgr;
    TickType mTickFreq;

    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

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
                        const TickType tickFreq,
                        const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
};


class CCachedSrcTransferGen : public CScheduleable, public CBaseOnDeletionInsert
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CFixedTimeTransferManager> mTransferMgr;
    TickType mTickFreq;

    bool ExistsFileAtStorageElement(const std::shared_ptr<SFile>& file, const CStorageElement* storageElement) const;
    void ExpireReplica(CStorageElement* storageElement, const TickType now);

public:
    struct SCacheElementInfo
    {
        std::size_t mCacheSize;
        TickType mDefaultReplicaLifetime;
        CStorageElement* mStorageElement;
    };

    CCachedSrcTransferGen(IBaseSim* sim,
                        std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                        const std::size_t numPerDay,
                        const TickType defaultReplicaLifetime,
                        const TickType tickFreq,
                        const TickType startTick=0 );

    std::vector<std::pair<float, std::vector<std::weak_ptr<SFile>>>> mRatiosAndFilesPerAccessCount{ {0.62f, {}}, {0.16f, {}}, {0.08f, {}}, {0.05f, {}} };
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<SCacheElementInfo> mCacheElements;
    std::vector<CStorageElement*> mDstStorageElements;
    std::size_t mNumPerDay;
    TickType mDefaultReplicaLifetime;

    void OnUpdate(const TickType now) final;
    void Shutdown(const TickType now) final;

    void OnFileCreated(const TickType now, std::shared_ptr<SFile> file) final;
};


class CHeartbeat : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CBaseTransferManager> mG2CTransferMgr;
    std::shared_ptr<CBaseTransferManager> mC2CTransferMgr;
    TickType mTickFreq;

    std::chrono::high_resolution_clock::time_point mTimeLastUpdate;

public:
    std::unordered_map<std::string, std::chrono::duration<double>*> mProccessDurations;

public:
    CHeartbeat(IBaseSim* sim, std::shared_ptr<CBaseTransferManager> g2cTransferMgr, std::shared_ptr<CBaseTransferManager> c2cTransferMgr, const TickType tickFreq, const TickType startTick = 0);

    void OnUpdate(const TickType now) final;
};
