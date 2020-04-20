#pragma once

#include <forward_list>
#include <list>
#include <unordered_map>

#include "CScheduleable.hpp"

#include "infrastructure/IActionListener.hpp"

class IBaseSim;
class CNetworkLink;
class IValueGenerator;
class CTransferManager;
class CFixedTimeTransferManager;
class IPreparedInsert;
class IInsertValuesContainer;
class CStorageElement;


class CBaseOnDeletionInsert : public IRucioActionListener, public IStorageElementActionListener
{
protected:
    std::unique_ptr<IInsertValuesContainer> mFileValueContainer;
    std::unique_ptr<IInsertValuesContainer> mReplicaValueContainer;

    std::shared_ptr<IPreparedInsert> mFileInsertQuery;
    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

    void AddFileDelete(SFile* file);
    void AddReplicaDelete(SReplica* replica);

public:
    CBaseOnDeletionInsert();

    void PostCreateFile(SFile* file, TickType now) override;
    void PreRemoveFile(SFile* file, TickType now) override;
    void PostCreateReplica(SReplica* replica, TickType now) override;
    void PreRemoveReplica(SReplica* replica, TickType now) override;
};



class CBufferedOnDeletionInsert : public CBaseOnDeletionInsert
{
private:
    void FlushFileDeletes();
    void FlushReplicaDeletes();

public:
    virtual ~CBufferedOnDeletionInsert();

    void PreRemoveFile(SFile* file, TickType now) override;
    void PreRemoveReplica(SReplica* replica, TickType now) override;
};



class CCloudBufferTransferGen : public CScheduleable, public IStorageElementActionListener
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    TickType mTickFreq;

public:
    struct STransferGenInfo
    {
        std::unique_ptr<IValueGenerator> mReusageNumGen;
        CNetworkLink* mPrimaryLink;
        CNetworkLink* mSecondaryLink;
        std::forward_list<SReplica*> mReplicas;
    };
    std::vector<std::unique_ptr<STransferGenInfo>> mTransferGenInfo;

    bool mDeleteSrcReplica = false;

    void PostCreateReplica(SReplica* replica, TickType now) override;
    void PreRemoveReplica(SReplica* replica, TickType now) override;

public:
    CCloudBufferTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        const TickType tickFreq,
                        const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
    void Shutdown(const TickType now) final;
};



class CJobIOTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;

    TickType mTickFreq;
    TickType mLastUpdateTime = 0;

    std::shared_ptr<IPreparedInsert> mTraceInsertQuery;

public:
    struct SJobInfo
    {
        IdType mJobId;
        TickType mStartedAt = 0;
        TickType mFinishedAt = 0;
        SpaceType mCurInputFileSize = 0;
        SFile* mInputFile;
        std::vector<SReplica*> mOutputReplicas;
    };
    struct SSiteInfo
    {
        CNetworkLink* mCloudToDiskLink;
        CNetworkLink* mDiskToCPULink;
        CNetworkLink* mCPUToOutputLink;
        std::unique_ptr<IValueGenerator> mJobDurationGen;
        std::unique_ptr<IValueGenerator> mNumOutputGen;
        std::unique_ptr<IValueGenerator> mOutputSizeGen;
        std::size_t mNumCores;
        std::size_t mCoreFillRate;
        std::list<SJobInfo> mJobInfos;
    };
    std::vector<SSiteInfo> mSiteInfos;

    CJobIOTransferGen(IBaseSim* sim,
                    std::shared_ptr<CTransferManager> transferMgr,
                    TickType tickFreq,
                    TickType startTick=0 );

    void OnUpdate(TickType now) final;
};



class CJobSlotTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CFixedTimeTransferManager> mTransferMgr;
    TickType mTickFreq;

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
                        TickType tickFreq,
                        TickType startTick=0 );

    void OnUpdate(TickType now) final;
};



class CCachedSrcTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CFixedTimeTransferManager> mTransferMgr;
    TickType mTickFreq;

    bool ExistsFileAtStorageElement(const SFile* file, const CStorageElement* storageElement) const;
    void ExpireReplica(CStorageElement* storageElement, TickType now);

public:
    struct SCacheElementInfo
    {
        std::size_t mCacheSize;
        TickType mDefaultReplicaLifetime;
        CStorageElement* mStorageElement;
    };

    CCachedSrcTransferGen(IBaseSim* sim,
                        std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                        std::size_t numPerDay,
                        TickType defaultReplicaLifetime,
                        TickType tickFreq,
                        TickType startTick=0 );

    std::vector<std::pair<float, std::vector<SFile*>>> mRatiosAndFilesPerAccessCount{ {0.62f, {}}, {0.16f, {}}, {0.08f, {}}, {0.05f, {}} };
    std::vector<CStorageElement*> mSrcStorageElements;
    std::vector<SCacheElementInfo> mCacheElements;
    std::vector<CStorageElement*> mDstStorageElements;
    std::size_t mNumPerDay;
    TickType mDefaultReplicaLifetime;

    void OnUpdate(TickType now) final;
    void Shutdown(TickType now) final;
};
