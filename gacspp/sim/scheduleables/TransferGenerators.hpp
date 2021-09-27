#pragma once

//#include <deque>
#include <forward_list>
#include <list>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "CScheduleable.hpp"

#include "infrastructure/IActionListener.hpp"
#include "infrastructure/SFile.hpp"

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

    void PostCompleteReplica(SReplica* replica, TickType now) override;
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



class CHCDCTransferGen : public CScheduleable, public IStorageElementActionListener
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;

    TickType mTickFreq;
    TickType mLastUpdateTime = 0;

    std::shared_ptr<IPreparedInsert> mInputTraceInsertQuery;
    std::shared_ptr<IPreparedInsert> mJobTraceInsertQuery;
    std::shared_ptr<IPreparedInsert> mOutputTraceInsertQuery;

public:
    struct SJobInfo
    {
        IdType mJobId;
        
        TickType mCreatedAt = 0;
        TickType mQueuedAt = 0;
        TickType mLastTime = 0;

        SpaceType mCurInputFileSize = 0;
        SFile* mInputFile = nullptr;
        SReplica* mInputReplica = nullptr;
        std::vector<SReplica*> mOutputReplicas;
    };
    
    typedef std::list<std::unique_ptr<SJobInfo>> JobInfoList;


    // configuration data / initialised by config
    CStorageElement* mHotStorageElement = nullptr;
    CStorageElement* mColdStorageElement = nullptr;
    CStorageElement* mArchiveStorageElement = nullptr;

    CNetworkLink* mArchiveToHotLink = nullptr;
    CNetworkLink* mArchiveToColdLink = nullptr;

    CNetworkLink* mHotToCPULink = nullptr;
    CNetworkLink* mCPUToOutputLink = nullptr;

    TickType mProductionStartTime = 0;

    std::size_t mNumCores = 0;

    std::unique_ptr<IValueGenerator> mReusageNumGen;
    std::unique_ptr<IValueGenerator> mNumJobSubmissionGen;
    std::unique_ptr<IValueGenerator> mJobDurationGen;
    std::unique_ptr<IValueGenerator> mNumOutputGen;
    std::unique_ptr<IValueGenerator> mOutputSizeGen;

    // runtime data / initialised automatically / book keeping
    std::vector<std::vector<SFile*>> mArchiveFilesPerPopularity;
    std::map<std::uint32_t, SIndexedReplicas> mHotReplicasByPopularity;
    std::map<std::uint32_t, std::forward_list<SReplica*>>  mColdReplicasByPopularity;

    //hot replicas that will be deleted after their transfer to cold storage is done
    std::unordered_set<SReplica*> mHotReplicaDeletions;
    std::map<TickType, std::vector<SReplica*>> mHotReplicasDeletionQueue;

    JobInfoList mWaitingJobs;
    std::unordered_map <SReplica*, JobInfoList> mTransferringJobs;
    JobInfoList mQueuedJobs;

    JobInfoList mNewJobs;
    JobInfoList mDownloadingJobs;
    std::map<TickType, JobInfoList> mRunningJobs;
    JobInfoList mUploadingJobs;

    std::unordered_map <SFile*, std::vector<JobInfoList::iterator>> mWaitingForSameFile;

    std::size_t mNumJobs = 0;
    double mNumJobSubmissionAccu = 0;

private:
    std::discrete_distribution<std::size_t> GetPopularityIdxRNG();

    void QueueHotReplicasDeletion(SReplica* replica, TickType expireAt);
    SpaceType DeleteQueuedHotReplicas(TickType now);

    void UpdateProductionCampaign(TickType now);
    void UpdatePendingDeletions(TickType now);
    void UpdateWaitingJobs(TickType now);
    void UpdateActiveJobs(TickType now);
    void UpdateQueuedJobs(TickType now);
    void SubmitNewJobs(TickType now);

    void PrepareProductionCampaign(TickType now);

public:
    void PostCompleteReplica(SReplica* replica, TickType now) override;
    void PostCreateReplica(SReplica* replica, TickType now) override;
    void PreRemoveReplica(SReplica* replica, TickType now) override;

    CHCDCTransferGen(IBaseSim* sim,
        std::shared_ptr<CTransferManager> transferMgr,
        TickType tickFreq,
        TickType startTick = 0);

    void OnUpdate(TickType now) final;
    void Shutdown(const TickType now) final;
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

    void PostCompleteReplica(SReplica* replica, TickType now) override;
    void PostCreateReplica(SReplica* replica, TickType now) override;
    void PreRemoveReplica(SReplica* replica, TickType now) override;

public:
    CCloudBufferTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        const TickType tickFreq,
                        const TickType startTick=0 );
    ~CCloudBufferTransferGen();

    void OnUpdate(const TickType now) final;
};



class CJobIOTransferGen : public CScheduleable
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;

    TickType mTickFreq;
    TickType mLastUpdateTime = 0;

    std::shared_ptr<IPreparedInsert> mInputTraceInsertQuery;
    std::shared_ptr<IPreparedInsert> mJobTraceInsertQuery;
    std::shared_ptr<IPreparedInsert> mOutputTraceInsertQuery;

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
    
    typedef std::list<std::unique_ptr<SJobInfo>> JobInfoList;

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
        std::list<std::unique_ptr<SJobInfo>> mActiveJobs;
        std::list<std::pair<TickType, std::list<std::unique_ptr<SJobInfo>>>> mRunningJobs;
        std::size_t mNumRunningJobs = 0;
        double mDiskLimitThreshold = 0.0;
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
};



class CFixedTransferGen : public CScheduleable, public IStorageElementActionListener
{
private:
    IBaseSim* mSim;
    std::shared_ptr<CTransferManager> mTransferMgr;
    TickType mTickFreq;

    std::vector<SReplica*> mCompleteReplicas;

public:
    CFixedTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        TickType tickFreq,
                        TickType startTick=0 );

    struct STransferGenInfo
    {
        CStorageElement* mDstStorageElement = nullptr;
        std::unique_ptr<IValueGenerator> mNumTransferGen;
        double mDecimalAccu = 0;
    };

    std::vector<std::pair<CStorageElement*, std::vector<STransferGenInfo>>> mConfig;

    void PostCompleteReplica(SReplica* replica, TickType now) override;
    void PostCreateReplica(SReplica* replica, TickType now) override;
    void PreRemoveReplica(SReplica* replica, TickType now) override;

    void OnUpdate(TickType now) final;
    void Shutdown(const TickType now) final;
};
