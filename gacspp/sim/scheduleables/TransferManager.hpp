#pragma once

#include "CScheduleable.hpp"


class IPreparedInsert;
class CNetworkLink;
struct SFile;
struct SReplica;


class ITransferStartListener
{
public:
    virtual void OnTransferStarted(const std::weak_ptr<SReplica>& srcReplica, const std::weak_ptr<SReplica>& dstReplica, CNetworkLink* networkLink) = 0;
};
class ITransferStopListener
{
public:
    virtual void OnTransferStopped(const std::weak_ptr<SReplica>& srcReplica, const std::weak_ptr<SReplica>& dstReplica, CNetworkLink* networkLink) = 0;
};



class CBaseTransferManager : public CScheduleable
{
public:
    using CScheduleable::CScheduleable;

    std::uint32_t mNumCompletedTransfers = 0;
    std::uint32_t mNumFailedTransfers = 0;
    TickType mSummedTransferDuration = 0;

    virtual auto GetNumActiveTransfers() const -> std::size_t = 0;

    std::vector<std::unique_ptr<ITransferStartListener>> mStartListeners;
    std::vector<std::unique_ptr<ITransferStopListener>> mStopListeners;
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
        bool mDeleteSrcReplica;

        STransfer(  std::shared_ptr<SReplica> srcReplica,
                    std::shared_ptr<SReplica> dstReplica,
                    CNetworkLink* const networkLink,
                    const TickType queuedAt,
                    const TickType startAt,
                    bool deleteSrcReplica);
    };

    std::vector<STransfer> mActiveTransfers;
    std::vector<STransfer> mQueuedTransfers;

public:
    CTransferManager(const TickType tickFreq, const TickType startTick=0);

    void OnUpdate(const TickType now) final;

    void CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now, bool deleteSrcReplica = false);

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