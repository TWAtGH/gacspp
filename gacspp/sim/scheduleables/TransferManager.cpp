#include <cassert>
#include <iostream>

#include "TransferManager.hpp"

#include "sim/IBaseSim.hpp"

#include "common/utils.hpp"

#include "infrastructure/CNetworkLink.hpp"
#include "infrastructure/CStorageElement.hpp"
#include "infrastructure/SFile.hpp"

#include "output/COutput.hpp"


bool CReplicaPreRemoveMultiListener::PreRemoveReplica(SReplica* replica, TickType now)
{
    auto prevIt = mListener.before_begin();
    auto curIt = mListener.begin();
    while (curIt != mListener.end())
    {
        if ((*curIt)->PreRemoveReplica(replica, now) == false)
        {
            curIt = mListener.erase_after(prevIt);
            continue;
        }
        prevIt++;
        curIt++;
    }
    return !mListener.empty();
}

void AddListener(SReplica* replica, IReplicaPreRemoveListener* listener)
{
    CReplicaPreRemoveMultiListener* specialListener;
    if (!replica->mRemoveListener)
    {
        specialListener = new CReplicaPreRemoveMultiListener;
        replica->mRemoveListener = specialListener;
    }
    else
        specialListener = dynamic_cast<CReplicaPreRemoveMultiListener*>(replica->mRemoveListener);

    assert(specialListener);

    specialListener->mListener.emplace_front(listener);
}

void RemoveListener(SReplica* replica, IReplicaPreRemoveListener* listener)
{
    auto specialListener = dynamic_cast<CReplicaPreRemoveMultiListener*>(replica->mRemoveListener);

    assert(specialListener);

    specialListener->mListener.remove(listener);
    if (specialListener->mListener.empty())
    {
        delete specialListener;
        replica->mRemoveListener = nullptr;
    }
}


CTransferManager::STransfer::STransfer( SReplica* srcReplica,
                                        SReplica* dstReplica,
                                        CNetworkLink* networkLink,
                                        TickType queuedAt,
                                        TickType startAt,
                                        bool deleteSrcReplica)
    : mSrcReplica(srcReplica),
      mDstReplica(dstReplica),
      mNetworkLink(networkLink),
      mQueuedAt(queuedAt),
      mActivatedAt(queuedAt),
      mStartAt(startAt),
      mDeleteSrcReplica(deleteSrcReplica)
{}

CTransferManager::STransfer::~STransfer()
{
    if(mSrcReplica && mSrcReplica->mRemoveListener)
        RemoveListener(mSrcReplica, this);
    if(mDstReplica && mDstReplica->mRemoveListener)
        RemoveListener(mDstReplica, this);
}

bool CTransferManager::STransfer::PreRemoveReplica(SReplica* replica, TickType now)
{
    (void)now;
    assert(mSrcReplica->mUsageCounter > 0);
    mSrcReplica->mUsageCounter -= 1;
    assert(mDstReplica->mUsageCounter > 0);
    mDstReplica->mUsageCounter -= 1;
    if (replica == mSrcReplica)
    {
        mSrcReplica = nullptr;
        return false;
    }
    if (replica == mDstReplica)
    {
        mDstReplica = nullptr;
        return false;
    }
    return true;
}

CTransferManager::CTransferManager(TickType tickFreq, TickType startTick)
    : CBaseTransferManager(startTick),
      mTickFreq(tickFreq)
{
    mOutputTransferInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Transfers(id, srcStorageElementId, dstStorageElementId, fileId, srcReplicaId, dstReplicaId, queuedAt, activatedAt, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 11, '?');
}

void CTransferManager::CreateTransfer(SReplica* srcReplica, SReplica* dstReplica, TickType now, bool deleteSrcReplica)
{
    srcReplica->mUsageCounter += 1;
    dstReplica->mUsageCounter += 1;

    CStorageElement* srcStorageElement = srcReplica->GetStorageElement();
    CNetworkLink* networkLink = srcStorageElement->GetNetworkLink( dstReplica->GetStorageElement() );

    std::unique_ptr<STransfer> newTransfer = std::make_unique<STransfer>(srcReplica, dstReplica, networkLink, now, now, deleteSrcReplica);

    AddListener(srcReplica, newTransfer.get());
    AddListener(dstReplica, newTransfer.get());

    auto res = mQueuedTransfers.insert({ networkLink, std::list<std::unique_ptr<STransfer>>() });
    res.first->second.emplace_back(std::move(newTransfer));
}

void CTransferManager::OnUpdate(TickType now)
{
    CScopedTimeDiffAdd durationUpdate(mUpdateDurationSummed);
    
    assert(now >= mLastUpdated);

    const TickType timeDiff = now - mLastUpdated;
    mLastUpdated = now;

    for (auto& queue : mQueuedTransfers)
    {
        CNetworkLink* networkLink = queue.first;
        std::list<std::unique_ptr<STransfer>>& queuedTransfers = queue.second;

        std::size_t numToCreate;
        if(networkLink->mMaxNumActiveTransfers > 0)
        {
            assert(networkLink->mNumActiveTransfers <= networkLink->mMaxNumActiveTransfers);
            numToCreate = networkLink->mMaxNumActiveTransfers - networkLink->mNumActiveTransfers;
        }
        else
            numToCreate = queuedTransfers.size();

        while (!queuedTransfers.empty() && (numToCreate > 0))
        {
            std::unique_ptr<STransfer>& curTransfer = queuedTransfers.front();

            curTransfer->mActivatedAt = now;
            curTransfer->mStartAt = now + networkLink->GetSrcStorageElement()->mAccessLatency->GetValue(IBaseSim::Sim->mRNGEngine);
            networkLink->mNumActiveTransfers += 1;
            networkLink->GetSrcStorageElement()->OnOperation(CStorageElement::GET);

            mActiveTransfers.emplace(curTransfer->mStartAt, std::move(curTransfer));

            numToCreate -= 1;
            queuedTransfers.pop_front();
        }
    }

    std::unique_ptr<IInsertValuesContainer> transferInsertQueries = mOutputTransferInsertQuery->CreateValuesContainer(7 + mActiveTransfers.size());

    std::multimap<TickType, std::unique_ptr<STransfer>>::iterator transferIt = mActiveTransfers.begin();
    while((transferIt != mActiveTransfers.end()) && (transferIt->first <= now))
    {
        std::unique_ptr<STransfer>& transfer = transferIt->second;
        
        SReplica* srcReplica = transfer->mSrcReplica;
        SReplica* dstReplica = transfer->mDstReplica;
        CNetworkLink* networkLink = transfer->mNetworkLink;

        if(!srcReplica || !dstReplica)
        {
            networkLink->mNumFailedTransfers += 1;
            networkLink->mNumActiveTransfers -= 1;
            transferIt = mActiveTransfers.erase(transferIt);
            continue;
        }

        SpaceType amount;
        if(!networkLink->mIsThroughput)
        {
            assert(networkLink->mNumActiveTransfers > 0);
            amount = (networkLink->mBandwidthBytesPerSecond / static_cast<double>(networkLink->mNumActiveTransfers)) * timeDiff;
        }
        else
            amount = networkLink->mBandwidthBytesPerSecond * timeDiff;
            
        amount = dstReplica->Increase(amount, now);
        networkLink->mUsedTraffic += amount;

        if(dstReplica->IsComplete())
        {
            transferInsertQueries->AddValue(GetNewId());
            transferInsertQueries->AddValue(srcReplica->GetStorageElement()->GetId());
            transferInsertQueries->AddValue(dstReplica->GetStorageElement()->GetId());
            transferInsertQueries->AddValue(srcReplica->GetFile()->GetId());
            transferInsertQueries->AddValue(srcReplica->GetId());
            transferInsertQueries->AddValue(dstReplica->GetId());
            transferInsertQueries->AddValue(transfer->mQueuedAt);
            transferInsertQueries->AddValue(transfer->mActivatedAt);
            transferInsertQueries->AddValue(transfer->mStartAt);
            transferInsertQueries->AddValue(now);
            transferInsertQueries->AddValue(dstReplica->GetCurSize());

            ++mNumCompletedTransfers;
            mSummedTransferDuration += 1 + now - transfer->mStartAt;

            networkLink->mNumDoneTransfers += 1;
            networkLink->mNumActiveTransfers -= 1;

            RemoveListener(srcReplica, transfer.get());
            RemoveListener(dstReplica, transfer.get());

            assert(srcReplica->mUsageCounter > 0);
            srcReplica->mUsageCounter -= 1;
            assert(dstReplica->mUsageCounter > 0);
            dstReplica->mUsageCounter -= 1;

            bool deleteSrc = transfer->mDeleteSrcReplica;

            transferIt = mActiveTransfers.erase(transferIt);

            if (deleteSrc)
            {
                assert(srcReplica->mUsageCounter == 0);
                srcReplica->GetStorageElement()->RemoveReplica(srcReplica, now, false);
            }

            continue;
        }
        ++transferIt;
    }

    COutput::GetRef().QueueInserts(std::move(transferInsertQueries));

    mNextCallTick = now + mTickFreq;
}

void CTransferManager::Shutdown(const TickType now)
{
    (void)now;
    mQueuedTransfers.clear();
    mActiveTransfers.clear();
}



CFixedTimeTransferManager::STransfer::STransfer( SReplica* srcReplica,
                                                 SReplica* dstReplica,
                                                 CNetworkLink* networkLink,
                                                 TickType queuedAt,
                                                 TickType startAt,
                                                 SpaceType increasePerTick)
    : mSrcReplica(srcReplica),
      mDstReplica(dstReplica),
      mNetworkLink(networkLink),
      mQueuedAt(queuedAt),
      mStartAt(startAt),
      mIncreasePerTick(increasePerTick)
{}

CFixedTimeTransferManager::STransfer::~STransfer()
{
    if (mSrcReplica->mRemoveListener)
        RemoveListener(mSrcReplica, this);
    if (mDstReplica->mRemoveListener)
        RemoveListener(mDstReplica, this);
}

bool CFixedTimeTransferManager::STransfer::PreRemoveReplica(SReplica* replica, TickType now)
{
    (void)now;
    assert(mSrcReplica->mUsageCounter > 0);
    mSrcReplica->mUsageCounter -= 1;
    assert(mDstReplica->mUsageCounter > 0);
    mDstReplica->mUsageCounter -= 1;
    if (replica == mSrcReplica)
    {
        mSrcReplica = nullptr;
        return false;
    }
    if (replica == mDstReplica)
    {
        mDstReplica = nullptr;
        return false;
    }
    return true;
}

CFixedTimeTransferManager::CFixedTimeTransferManager(TickType tickFreq, TickType startTick)
    : CBaseTransferManager(startTick),
      mTickFreq(tickFreq)
{
    mOutputTransferInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Transfers(id, srcStorageElementId, dstStorageElementId, fileId, srcReplicaId, dstReplicaId, queuedAt, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 10, '?');
}

void CFixedTimeTransferManager::CreateTransfer(SReplica* srcReplica, SReplica* dstReplica, TickType now, TickType startDelay, TickType duration)
{
    srcReplica->mUsageCounter += 1;
    dstReplica->mUsageCounter += 1;

    CStorageElement* srcStorageElement = srcReplica->GetStorageElement();
    CNetworkLink* networkLink = srcStorageElement->GetNetworkLink(dstReplica->GetStorageElement());

    SpaceType increasePerTick = static_cast<SpaceType>(static_cast<double>(srcReplica->GetFile()->GetSize()) / std::max<TickType>(1, duration));

    networkLink->mNumActiveTransfers += 1;
    srcStorageElement->OnOperation(CStorageElement::GET);

    mQueuedTransfers.emplace_back(std::make_unique<STransfer>(srcReplica, dstReplica, networkLink, now, now + startDelay, increasePerTick + 1));

    AddListener(srcReplica, mQueuedTransfers.back().get());
    AddListener(dstReplica, mQueuedTransfers.back().get());
}

void CFixedTimeTransferManager::OnUpdate(TickType now)
{
    CScopedTimeDiffAdd durationUpdate(mUpdateDurationSummed);

    std::size_t i = 0;
    while(i < mQueuedTransfers.size())
    {
        if (mQueuedTransfers[i]->mStartAt <= now)
        {
            mActiveTransfers.emplace_back(std::move(mQueuedTransfers[i]));
            mQueuedTransfers[i] = std::move(mQueuedTransfers.back());
            mQueuedTransfers.pop_back();
        }
        else
            ++i;
    }

    const std::uint32_t timeDiff = static_cast<std::uint32_t>(now - mLastUpdated);
    mLastUpdated = now;

    std::size_t idx = 0;
    auto transferInsertQueries = mOutputTransferInsertQuery->CreateValuesContainer(6 + mActiveTransfers.size());

    while (idx < mActiveTransfers.size())
    {
        std::unique_ptr<STransfer>& transfer = mActiveTransfers[idx];
        SReplica* srcReplica = transfer->mSrcReplica;
        SReplica* dstReplica = transfer->mDstReplica;
        CNetworkLink* networkLink = transfer->mNetworkLink;

        if(!srcReplica || !dstReplica)
        {
            networkLink->mNumFailedTransfers += 1;
            networkLink->mNumActiveTransfers -= 1;
            mNumFailedTransfers += 1;
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
            continue; // handle same idx again
        }

        SpaceType amount = dstReplica->Increase(transfer->mIncreasePerTick * timeDiff, now);
        networkLink->mUsedTraffic += amount;

        if(dstReplica->IsComplete())
        {
            transferInsertQueries->AddValue(GetNewId());
            transferInsertQueries->AddValue(srcReplica->GetStorageElement()->GetId());
            transferInsertQueries->AddValue(dstReplica->GetStorageElement()->GetId());
            transferInsertQueries->AddValue(srcReplica->GetFile()->GetId());
            transferInsertQueries->AddValue(srcReplica->GetId());
            transferInsertQueries->AddValue(dstReplica->GetId());
            transferInsertQueries->AddValue(transfer->mQueuedAt);
            transferInsertQueries->AddValue(transfer->mStartAt);
            transferInsertQueries->AddValue(now);
            transferInsertQueries->AddValue(dstReplica->GetCurSize());

            ++mNumCompletedTransfers;
            mSummedTransferDuration += now - transfer->mStartAt;

            networkLink->mNumDoneTransfers += 1;
            networkLink->mNumActiveTransfers -= 1;

            RemoveListener(srcReplica, transfer.get());
            RemoveListener(dstReplica, transfer.get());

            assert(srcReplica->mUsageCounter > 0);
            srcReplica->mUsageCounter -= 1;
            assert(dstReplica->mUsageCounter > 0);
            dstReplica->mUsageCounter -= 1;

            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
            continue; // handle same idx again
        }
        ++idx;
    }

    COutput::GetRef().QueueInserts(std::move(transferInsertQueries));

    mNextCallTick = now + mTickFreq;
}