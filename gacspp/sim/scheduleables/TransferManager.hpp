#pragma once

#include <forward_list>
#include <list>
#include <unordered_map>

#include "CScheduleable.hpp"
#include "infrastructure/IActionListener.hpp"

class IPreparedInsert;
class CNetworkLink;
struct SFile;
struct SReplica;



class CReplicaPreRemoveMultiListener : public IReplicaPreRemoveListener
{
public:
    std::forward_list<IReplicaPreRemoveListener*> mListener;
    virtual bool PreRemoveReplica(SReplica* replica, TickType now) override;
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
    struct STransfer// : public IReplicaPreRemoveListener
    {
        SReplica* mSrcReplica;
        SReplica* mDstReplica;

        TickType mQueuedAt;
        TickType mStartAt;

        std::size_t mCurRouteIdx = 0;

        STransfer(SReplica* srcReplica, SReplica* dstReplica, TickType queuedAt, TickType startedAt, std::size_t routeIdx = 0);

        //void PreRemoveReplica(const SReplica* replica, TickType now) override;
    };

    struct STransferBatch
    {
        std::vector<CNetworkLink*> mRoute;

        TickType mStartAt;

        std::vector<std::unique_ptr<STransfer>> mTransfers;
        std::uint32_t mNumDoneTransfers = 0;
    };

public:
    CTransferBatchManager(TickType tickFreq, TickType startTick=0);

    void OnUpdate(TickType now) final;

    void QueueTransferBatch(std::shared_ptr<STransferBatch> transferBatch)
    {
        if(!transferBatch->mTransfers.empty())
            mQueuedTransferBatches.emplace_back(transferBatch);
    }

    auto GetNumActiveTransfers() const -> std::size_t
    {return mActiveTransferBatches.size();}

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

    struct STransfer : public IReplicaPreRemoveListener
    {
        SReplica* mSrcReplica;
        SReplica* mDstReplica;
        CNetworkLink* mNetworkLink;
        TickType mQueuedAt;
        TickType mStartAt;
        bool mDeleteSrcReplica;

        STransfer(  SReplica* srcReplica,
                    SReplica* dstReplica,
                    CNetworkLink* networkLink,
                    TickType queuedAt,
                    TickType startAt,
                    bool deleteSrcReplica);
        ~STransfer();

        bool PreRemoveReplica(SReplica* replica, TickType now) override;
    };

    std::vector<std::unique_ptr<STransfer>> mActiveTransfers;
    std::unordered_map<CNetworkLink*, std::list<std::unique_ptr<STransfer>>> mQueuedTransfers;

public:
    CTransferManager(TickType tickFreq, TickType startTick = 0);

    void OnUpdate(TickType now) final;

    void CreateTransfer(SReplica* srcReplica, SReplica* dstReplica, TickType now, bool deleteSrcReplica = false);

    auto GetNumActiveTransfers() const -> std::size_t
    {return mActiveTransfers.size();}
};


class CFixedTimeTransferManager : public CBaseTransferManager
{
private:
    std::shared_ptr<IPreparedInsert> mOutputTransferInsertQuery;

    TickType mLastUpdated = 0;
    TickType mTickFreq;

    struct STransfer : public IReplicaPreRemoveListener
    {
        SReplica* mSrcReplica;
        SReplica* mDstReplica;
        CNetworkLink* mNetworkLink;
        TickType mQueuedAt;
        TickType mStartAt;

        SpaceType mIncreasePerTick;

        STransfer(  SReplica* srcReplica,
                    SReplica* dstReplica,
                    CNetworkLink* networkLink,
                    TickType queuedAt,
                    TickType startAt,
            SpaceType increasePerTick);
        ~STransfer();

        bool PreRemoveReplica(SReplica* replica, TickType now) override;
    };

    std::vector<std::unique_ptr<STransfer>> mActiveTransfers;
    std::vector<std::unique_ptr<STransfer>> mQueuedTransfers;

public:
    CFixedTimeTransferManager(TickType tickFreq, TickType startTick=0);

    void OnUpdate(TickType now) final;

    void CreateTransfer(SReplica* srcReplica, SReplica* dstReplica, TickType now, TickType startDelay, TickType duration);


    inline auto GetNumQueuedTransfers() const -> std::size_t
    {return mQueuedTransfers.size();}
    auto GetNumActiveTransfers() const -> std::size_t
    {return mActiveTransfers.size();}
};