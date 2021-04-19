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
        TickType mAccessLatency = 0;
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
    void Shutdown(const TickType now) final;

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