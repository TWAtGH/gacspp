#pragma once

#include <forward_list>
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


class CBaseOnDeletionInsert : public IFileActionListener, public IReplicaActionListener
{
protected:
    std::unique_ptr<IInsertValuesContainer> mFileValueContainer;
    std::unique_ptr<IInsertValuesContainer> mReplicaValueContainer;

    std::shared_ptr<IPreparedInsert> mFileInsertQuery;
    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

    void AddFileDeletes(const std::vector<std::weak_ptr<SFile>>& deletedFiles);
    void AddReplicaDelete(const std::weak_ptr<SReplica>& replica);

public:
    CBaseOnDeletionInsert();

    void OnFileCreated(const TickType now, std::shared_ptr<SFile> file) override;
    void OnFilesDeleted(const TickType now, const std::vector<std::weak_ptr<SFile>>& deletedFiles) override;
    void OnReplicaCreated(const TickType now, std::shared_ptr<SReplica> replica) override;
    void OnReplicaDeleted(const TickType now, std::weak_ptr<SReplica> replica) override;
};



class CBufferedOnDeletionInsert : public CBaseOnDeletionInsert
{
private:
    void FlushFileDeletes();
    void FlushReplicaDeletes();

public:
    virtual ~CBufferedOnDeletionInsert();
    void OnFilesDeleted(const TickType now, const std::vector<std::weak_ptr<SFile>>& deletedFiles) override;
    void OnReplicaDeleted(const TickType now, std::weak_ptr<SReplica> replica) override;
};



class CCloudBufferTransferGen : public CScheduleable, public IReplicaActionListener
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
        std::forward_list<std::pair<std::uint32_t, std::shared_ptr<SReplica>>> mReplicaInfo;
    };
    std::vector<std::unique_ptr<STransferGenInfo>> mTransferGenInfo;

    bool mDeleteSrcReplica = false;

    void OnReplicaCreated(const TickType now, std::shared_ptr<SReplica> replica) override;
    void OnReplicaDeleted(const TickType now, std::weak_ptr<SReplica> replica) override;

public:
    CCloudBufferTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        const TickType tickFreq,
                        const TickType startTick=0 );

    void OnUpdate(const TickType now) final;
    void Shutdown(const TickType now) final;
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
