#pragma once

#include <chrono>
#include <unordered_map>

#include "constants.h"
#include "CScheduleable.hpp"

class IBaseSim;
class CRucio;
class CStorageElement;
class CNetworkLink;
struct SReplica;

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
    std::shared_ptr<IPreparedInsert> mOutputFileInsertQuery;

    IBaseSim* mSim;

    std::unique_ptr<IValueGenerator> mNumFilesRNG;
    std::unique_ptr<IValueGenerator> mFileSizeRNG;
    std::unique_ptr<IValueGenerator> mFileLifetimeRNG;

    std::uint32_t mTickFreq;

    std::uint32_t GetRandomFileSize();
    std::uint32_t GetRandomNumFilesToGenerate();
    TickType GetRandomLifeTime();

public:

    bool mSelectStorageElementsRandomly = false;
    std::vector<float> mNumReplicaRatio;
    std::vector<CStorageElement*> mStorageElements;

    CDataGenerator( IBaseSim* sim,
                    std::unique_ptr<IValueGenerator>&& numFilesRNG,
                    std::unique_ptr<IValueGenerator>&& fileSizeRNG,
                    std::unique_ptr<IValueGenerator>&& fileLifetimeRNG,
                    const std::uint32_t tickFreq,
                    const TickType startTick=0);

    std::uint64_t CreateFilesAndReplicas(const std::uint32_t numFiles, const std::uint32_t numReplicasPerFile, const TickType now);

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
	std::shared_ptr<IPreparedInsert> mCloudBillInsertQuery;

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
		CNetworkLink* mNetworkLink;
		TickType mQueuedAt;
		TickType mStartAt;

        STransfer(  std::shared_ptr<SReplica> srcReplica,
                    std::shared_ptr<SReplica> dstReplica,
					CNetworkLink* const networkLink,
					const TickType queudAt,
					const TickType startAt);
    };

    std::vector<STransfer> mActiveTransfers;
	std::vector<STransfer> mQueuedTransfers;

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
		CNetworkLink* mNetworkLink;
		TickType mQueuedAt;
		TickType mStartAt;

        std::uint32_t mIncreasePerTick;

        STransfer(  std::shared_ptr<SReplica> srcReplica,
                    std::shared_ptr<SReplica> dstReplica,
					CNetworkLink* const networkLink,
					const TickType queuedAt,
                    const TickType startAt,
                    const std::uint32_t increasePerTick);
    };

    std::vector<STransfer> mActiveTransfers;
    std::vector<STransfer> mQueuedTransfers;

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


class CUniformTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    std::uint32_t mTickFreq;

public:
    std::shared_ptr<CWavedTransferNumGen> mTransferNumGen;
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

public:
    CUniformTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        std::shared_ptr<CWavedTransferNumGen> transferNumGen,
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
    std::shared_ptr<CWavedTransferNumGen> mTransferNumGen;
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<CStorageElement*> mDstStorageElements;

public:
    CExponentialTransferGen(IBaseSim* sim,
                            std::shared_ptr<CTransferManager> transferMgr,
                            std::shared_ptr<CWavedTransferNumGen> transferNumGen,
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
    std::shared_ptr<CWavedTransferNumGen> mTransferNumGen;
    std::unordered_map<IdType, int> mSrcStorageElementIdToPrio;
    std::vector<CStorageElement*> mDstStorageElements;

public:
    CSrcPrioTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        std::shared_ptr<CWavedTransferNumGen> transferNumGen,
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


class CCachedSrcTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CFixedTimeTransferManager> mTransferMgr;
    std::uint32_t mTickFreq;

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
                        const TickType defaultReplicaLifetime,
                        const std::uint32_t tickFreq,
                        const TickType startTick=0 );

    std::vector<std::pair<std::size_t, std::vector<std::weak_ptr<SFile>>>> mRatiosAndFilesPerAccessCount{ {50, {}}, {15, {}}, {4, {}}, {1, {}} };
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<SCacheElementInfo> mCacheElements;
    std::vector<CStorageElement*> mDstStorageElements;
    TickType mDefaultReplicaLifetime;

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
