#include <iostream>

#include "TransferManager.hpp"

#include "common/utils.hpp"

#include "infrastructure/CNetworkLink.hpp"
#include "infrastructure/CStorageElement.hpp"
#include "infrastructure/SFile.hpp"

#include "output/COutput.hpp"

CTransferBatchManager::STransfer::STransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, TickType queuedAt, TickType startedAt, std::size_t routeIdx)
    : mSrcReplica(srcReplica),
      mDstReplica(dstReplica),
      mQueuedAt(queuedAt),
      mStartAt(startedAt),
      mCurRouteIdx(routeIdx)
{}

CTransferBatchManager::CTransferBatchManager(const TickType tickFreq, const TickType startTick)
    : CBaseTransferManager(startTick),
      mTickFreq(tickFreq)
{}

void CTransferBatchManager::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    std::size_t i = 0;
    while(i < mQueuedTransferBatches.size())
    {
        if (mQueuedTransferBatches[i]->mStartAt <= now)
        {
            mQueuedTransferBatches[i]->mRoute[0]->mNumActiveTransfers += mQueuedTransferBatches[i]->mTransfers.size();
            mActiveTransferBatches.emplace_back(std::move(mQueuedTransferBatches[i]));
            mQueuedTransferBatches[i] = std::move(mQueuedTransferBatches.back());
            mQueuedTransferBatches.pop_back();
        }
        else
            ++i;
    }

    const std::uint32_t timeDiff = static_cast<std::uint32_t>(now - mLastUpdated);
    mLastUpdated = now;

    std::size_t batchIdx = 0;
    std::unique_ptr<IInsertValuesContainer> transferInsertQueries = mOutputTransferInsertQuery->CreateValuesContainer();

    while (batchIdx < mActiveTransferBatches.size())
    {
        std::shared_ptr<STransferBatch>& batch = mActiveTransferBatches[batchIdx];
        std::vector<std::unique_ptr<STransfer>>& transfers = batch->mTransfers;

        std::size_t transferIdx = 0;

        while(transferIdx < transfers.size())
        {
            std::unique_ptr<STransfer>& transfer = transfers[transferIdx];
            std::shared_ptr<SReplica> srcReplica = transfer->mSrcReplica;
            std::shared_ptr<SReplica> dstReplica = transfer->mDstReplica;
            CNetworkLink* const networkLink = batch->mRoute[transfer->mCurRouteIdx];

            const double sharedBandwidth = networkLink->mBandwidthBytesPerSecond / static_cast<double>(networkLink->mNumActiveTransfers);
            SpaceType amount = static_cast<SpaceType>(sharedBandwidth * timeDiff);
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
                transferInsertQueries->AddValue(transfer->mStartAt);
                transferInsertQueries->AddValue(now);
                transferInsertQueries->AddValue(dstReplica->GetCurSize());

                networkLink->mNumDoneTransfers += 1;
                networkLink->mNumActiveTransfers -= 1;

                mNumCompletedTransfers += 1;
                mSummedTransferDuration += now - transfer->mStartAt;

                std::size_t nextRouteIdx = transfer->mCurRouteIdx + 1;
                while(nextRouteIdx < batch->mRoute.size())
                {
                    CStorageElement* dstStorageElement = batch->mRoute[nextRouteIdx]->GetDstStorageElement();
                    std::shared_ptr<SReplica> replica = dstReplica->GetFile()->GetReplicaByStorageElement(dstStorageElement);
                    if(!replica)
                    {
                        batch->mRoute[nextRouteIdx]->mNumActiveTransfers += 1;
                        replica = dstStorageElement->CreateReplica(dstReplica->GetFile(), now);
                        transfers.emplace_back(std::make_unique<STransfer>(dstReplica, replica, now, now, nextRouteIdx));

                        ++transferIdx; // dont handle new transfer immediately
                        break;
                    }
                    else
                    {
                        dstReplica = replica;
                        //store replica if it is used as src for next transfer
                        nextRouteIdx += 1;
                    }
                }
                //dstReplica.use_count()

                if(nextRouteIdx >= batch->mRoute.size())
                    batch->mNumDoneTransfers += 1;

                transfer = std::move(transfers.back());
                transfers.pop_back();
                continue; // handle same idx again
            }

            ++transferIdx;
        }

        if(transfers.empty())
        {
            batch = std::move(mActiveTransferBatches.back());
            mActiveTransferBatches.pop_back();
            continue; // handle same idx again
        }

        ++batchIdx;
    }

    COutput::GetRef().QueueInserts(std::move(transferInsertQueries));

    mNextCallTick = now + mTickFreq;
}



CTransferManager::STransfer::STransfer( std::shared_ptr<SReplica> srcReplica,
                                        std::shared_ptr<SReplica> dstReplica,
                                        CNetworkLink* const networkLink,
                                        const TickType queuedAt,
                                        const TickType startAt,
                                        bool deleteSrcReplica)
    : mSrcReplica(srcReplica),
      mDstReplica(dstReplica),
      mNetworkLink(networkLink),
      mQueuedAt(queuedAt),
      mStartAt(startAt),
      mDeleteSrcReplica(deleteSrcReplica)
{}

CTransferManager::CTransferManager(const TickType tickFreq, const TickType startTick)
    : CBaseTransferManager(startTick),
      mTickFreq(tickFreq)
{
    mActiveTransfers.reserve(1024*1024);
    mQueuedTransfers.reserve(1024 * 1024);
    mOutputTransferInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Transfers(id, srcStorageElementId, dstStorageElementId, fileId, srcReplicaId, dstReplicaId, queuedAt, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 10, '?');
}

void CTransferManager::CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now, bool deleteSrcReplica)
{
    assert(mQueuedTransfers.size() < mQueuedTransfers.capacity());

    CStorageElement* const srcStorageElement = srcReplica->GetStorageElement();
    CNetworkLink* const networkLink = srcStorageElement->GetNetworkLink( dstReplica->GetStorageElement() );

    networkLink->mNumActiveTransfers += 1;
    srcStorageElement->OnOperation(CStorageElement::GET);
    mQueuedTransfers.emplace_back(srcReplica, dstReplica, networkLink, now, now, deleteSrcReplica);
}

void CTransferManager::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    //std::cout<<mName<<": "<<mActiveTransfers.size()<<" / "<<mQueuedTransfers.size()<<std::endl;

    const std::uint32_t timeDiff = static_cast<std::uint32_t>(now - mLastUpdated);
    mLastUpdated = now;


    for (std::size_t i = 0; i < mQueuedTransfers.size(); ++i)
    {
        if (mQueuedTransfers[i].mStartAt <= now)
        {
            assert(mActiveTransfers.size() < mActiveTransfers.capacity());
            mActiveTransfers.emplace_back(std::move(mQueuedTransfers[i]));
            mQueuedTransfers[i] = std::move(mQueuedTransfers.back());
            mQueuedTransfers.pop_back();
        }
    }

    std::size_t idx = 0;
    std::unique_ptr<IInsertValuesContainer> transferInsertQueries = mOutputTransferInsertQuery->CreateValuesContainer(6 + mActiveTransfers.size());

    while (idx < mActiveTransfers.size())
    {
        STransfer& transfer = mActiveTransfers[idx];
        std::shared_ptr<SReplica> srcReplica = transfer.mSrcReplica.lock();
        std::shared_ptr<SReplica> dstReplica = transfer.mDstReplica.lock();
        CNetworkLink* const networkLink = transfer.mNetworkLink;

        if(!srcReplica || !dstReplica)
        {
            networkLink->mNumFailedTransfers += 1;
            networkLink->mNumActiveTransfers -= 1;
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
            continue; // handle same idx again
        }

        const double sharedBandwidth = networkLink->mBandwidthBytesPerSecond / static_cast<double>(networkLink->mNumActiveTransfers);
        std::uint32_t amount = static_cast<std::uint32_t>(sharedBandwidth * timeDiff);
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
            transferInsertQueries->AddValue(transfer.mQueuedAt);
            transferInsertQueries->AddValue(transfer.mStartAt);
            transferInsertQueries->AddValue(now);
            transferInsertQueries->AddValue(dstReplica->GetCurSize());

            ++mNumCompletedTransfers;
            mSummedTransferDuration += now - transfer.mStartAt;

            networkLink->mNumDoneTransfers += 1;
            networkLink->mNumActiveTransfers -= 1;
            if(transfer.mDeleteSrcReplica)
                srcReplica->GetFile()->RemoveReplica(now, srcReplica);

            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
            continue; // handle same idx again
        }
        ++idx;
    }

    COutput::GetRef().QueueInserts(std::move(transferInsertQueries));

    mNextCallTick = now + mTickFreq;
}



CFixedTimeTransferManager::STransfer::STransfer( std::shared_ptr<SReplica> srcReplica,
                                                 std::shared_ptr<SReplica> dstReplica,
                                                 CNetworkLink* const networkLink,
                                                 const TickType queuedAt,
                                                 const TickType startAt,
                                                 const SpaceType increasePerTick)
    : mSrcReplica(srcReplica),
      mDstReplica(dstReplica),
      mNetworkLink(networkLink),
      mQueuedAt(queuedAt),
      mStartAt(startAt),
      mIncreasePerTick(increasePerTick)
{}

CFixedTimeTransferManager::CFixedTimeTransferManager(const TickType tickFreq, const TickType startTick)
    : CBaseTransferManager(startTick),
      mTickFreq(tickFreq)
{
    mActiveTransfers.reserve(1024 * 1024);
    mQueuedTransfers.reserve(1024 * 1024);
    mOutputTransferInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Transfers(id, srcStorageElementId, dstStorageElementId, fileId, srcReplicaId, dstReplicaId, queuedAt, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 10, '?');
}

void CFixedTimeTransferManager::CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now, const TickType startDelay, const TickType duration)
{
    assert(mQueuedTransfers.size() < mQueuedTransfers.capacity());

    CStorageElement* const srcStorageElement = srcReplica->GetStorageElement();
    CNetworkLink* const networkLink = srcStorageElement->GetNetworkLink(dstReplica->GetStorageElement());

    SpaceType increasePerTick = static_cast<SpaceType>(static_cast<double>(srcReplica->GetFile()->GetSize()) / std::max<TickType>(1, duration));

    networkLink->mNumActiveTransfers += 1;
    srcStorageElement->OnOperation(CStorageElement::GET);
    mQueuedTransfers.emplace_back(srcReplica, dstReplica, networkLink, now, now+startDelay, increasePerTick+1);
}

void CFixedTimeTransferManager::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    std::size_t i = 0;
    while(i < mQueuedTransfers.size())
    {
        if (mQueuedTransfers[i].mStartAt <= now)
        {
            assert(mActiveTransfers.size() < mActiveTransfers.capacity());
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
        STransfer& transfer = mActiveTransfers[idx];
        std::shared_ptr<SReplica> srcReplica = transfer.mSrcReplica.lock();
        std::shared_ptr<SReplica> dstReplica = transfer.mDstReplica.lock();
        CNetworkLink* const networkLink = transfer.mNetworkLink;

        if(!srcReplica || !dstReplica)
        {
            networkLink->mNumFailedTransfers += 1;
            networkLink->mNumActiveTransfers -= 1;
            mNumFailedTransfers += 1;
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
            continue; // handle same idx again
        }

        SpaceType amount = dstReplica->Increase(transfer.mIncreasePerTick * timeDiff, now);
        networkLink->mUsedTraffic += amount;

        if(dstReplica->IsComplete())
        {
            transferInsertQueries->AddValue(GetNewId());
            transferInsertQueries->AddValue(srcReplica->GetStorageElement()->GetId());
            transferInsertQueries->AddValue(dstReplica->GetStorageElement()->GetId());
            transferInsertQueries->AddValue(srcReplica->GetFile()->GetId());
            transferInsertQueries->AddValue(srcReplica->GetId());
            transferInsertQueries->AddValue(dstReplica->GetId());
            transferInsertQueries->AddValue(transfer.mQueuedAt);
            transferInsertQueries->AddValue(transfer.mStartAt);
            transferInsertQueries->AddValue(now);
            transferInsertQueries->AddValue(dstReplica->GetCurSize());

            ++mNumCompletedTransfers;
            mSummedTransferDuration += now - transfer.mStartAt;

            networkLink->mNumDoneTransfers += 1;
            networkLink->mNumActiveTransfers -= 1;
            transfer = std::move(mActiveTransfers.back());
            mActiveTransfers.pop_back();
            continue; // handle same idx again
        }
        ++idx;
    }

    COutput::GetRef().QueueInserts(std::move(transferInsertQueries));

    mNextCallTick = now + mTickFreq;
}

void CFixedTimeTransferManager::Shutdown(const TickType now)
{
    std::size_t numWithDst=0, numWithoutDst=0;
    SpaceType sumCurSizes=0, sumFileSizes=0;
    for(const STransfer& transfer : mQueuedTransfers)
    {
        std::shared_ptr<SReplica> dstReplica = transfer.mDstReplica.lock();
        if(dstReplica)
        {
            numWithDst += 1;
            sumCurSizes += dstReplica->GetCurSize();
            sumFileSizes += dstReplica->GetFile()->GetSize();
        }
        else
            numWithoutDst += 1;
    }
    std::cout<<"Queued: numWithDst="<<numWithDst<<"; numWithoutDst="<<numWithoutDst;
    std::cout<<"; sumCurSizes="<<sumCurSizes<<"; sumFileSizes="<<sumFileSizes<<std::endl;

    numWithDst = numWithoutDst = sumCurSizes = sumFileSizes = 0;
    for(const STransfer& transfer : mActiveTransfers)
    {
        std::shared_ptr<SReplica> dstReplica = transfer.mDstReplica.lock();
        if(dstReplica)
        {
            numWithDst += 1;
            sumCurSizes += dstReplica->GetCurSize();
            sumFileSizes += dstReplica->GetFile()->GetSize();
        }
        else
            numWithoutDst += 1;
    }
    std::cout<<"Active: numWithDst="<<numWithDst<<"; numWithoutDst="<<numWithoutDst;
    std::cout<<"; sumCurSizes="<<sumCurSizes<<"; sumFileSizes="<<sumFileSizes<<std::endl;
}