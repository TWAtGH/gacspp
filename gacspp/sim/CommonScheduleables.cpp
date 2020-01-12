#include <algorithm>
#include <cassert>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>

#include "CommonScheduleables.hpp"
#include "IBaseSim.hpp"

#include "clouds/IBaseCloud.hpp"

#include "infrastructure/CNetworkLink.hpp"
#include "infrastructure/CRucio.hpp"
#include "infrastructure/CStorageElement.hpp"
#include "infrastructure/SFile.hpp"

#include "output/COutput.hpp"



CScopedTimeDiff::CScopedTimeDiff(std::chrono::duration<double>& outVal, bool willAdd)
    :mStartTime(std::chrono::high_resolution_clock::now()), mOutVal(outVal), mWillAdd(willAdd)
{}

CScopedTimeDiff::~CScopedTimeDiff()
{
    const auto duration = std::chrono::high_resolution_clock::now() - mStartTime;
    if (mWillAdd)
        mOutVal += duration;
    else
        mOutVal = duration;
}



CFixedValueGenerator::CFixedValueGenerator(const double value)
    : mValue(value)
{}

auto CFixedValueGenerator::GetValue(RNGEngineType& rngEngine) -> double
{
    (void)rngEngine;
    return mValue;
}


CNormalRandomValueGenerator::CNormalRandomValueGenerator(const double mean, const double stddev)
    : mNormalRNGDistribution(mean, stddev)
{}

auto CNormalRandomValueGenerator::GetValue(RNGEngineType& rngEngine) -> double
{
    return mNormalRNGDistribution(rngEngine);
}


CDataGenerator::CDataGenerator( IBaseSim* sim,
                                std::unique_ptr<IValueGenerator>&& numFilesRNG,
                                std::unique_ptr<IValueGenerator>&& fileSizeRNG,
                                std::unique_ptr<IValueGenerator>&& fileLifetimeRNG,
                                const TickType tickFreq,
                                const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mNumFilesRNG(std::move(numFilesRNG)),
      mFileSizeRNG(std::move(fileSizeRNG)),
      mFileLifetimeRNG(std::move(fileLifetimeRNG)),
      mTickFreq(tickFreq)
{}

auto CDataGenerator::GetRandomFileSize() const -> SpaceType
{
    assert(mFileSizeRNG);
    constexpr double min = 512000; //0.5 KiB
    constexpr double max = static_cast<double>(std::numeric_limits<SpaceType>::max());
    const double val = min + GiB_TO_BYTES(std::abs(mFileSizeRNG->GetValue(mSim->mRNGEngine)));
    return static_cast<SpaceType>(std::min(val, max));
}

auto CDataGenerator::GetRandomNumFilesToGenerate() const -> std::uint32_t
{
    assert(mNumFilesRNG);
    return static_cast<std::uint32_t>( std::max(1.0, mNumFilesRNG->GetValue(mSim->mRNGEngine)) );
}

auto CDataGenerator::GetRandomLifeTime() const -> TickType
{
    assert(mFileLifetimeRNG);
    float val = DAYS_TO_SECONDS(std::abs(mFileLifetimeRNG->GetValue(mSim->mRNGEngine)));
    return static_cast<TickType>( std::max(float(SECONDS_PER_DAY), val) );
}

void  CDataGenerator::CreateFilesAndReplicas(const std::uint32_t numFiles, const std::uint32_t numReplicasPerFile, const TickType now)
{
    if(numFiles == 0 || numReplicasPerFile == 0)
        return;

    const std::uint32_t numStorageElements = static_cast<std::uint32_t>( mStorageElements.size() );

    assert(numReplicasPerFile <= numStorageElements);

    std::uniform_int_distribution<std::uint32_t> rngSampler(0, numStorageElements);
    for(std::uint32_t i = 0; i < numFiles; ++i)
    {
        const std::uint32_t fileSize = GetRandomFileSize();
        const TickType lifetime = GetRandomLifeTime();

        std::shared_ptr<SFile> file = mSim->mRucio->CreateFile(fileSize, now, lifetime);

        auto reverseRSEIt = mStorageElements.rbegin();
        auto selectedElementIt = mStorageElements.begin();
        //numReplicasPerFile <= numStorageElements !
        for(std::uint32_t numCreated = 0; numCreated<numReplicasPerFile; ++numCreated)
        {
            if(mSelectStorageElementsRandomly)
                selectedElementIt = mStorageElements.begin() + (rngSampler(mSim->mRNGEngine) % (numStorageElements - numCreated));

            std::shared_ptr<SReplica> r = (*selectedElementIt)->CreateReplica(file, now);

            r->Increase(fileSize, now);
            r->mExpiresAt = now + (lifetime / numReplicasPerFile);

            if(mSelectStorageElementsRandomly)
            {
                std::iter_swap(selectedElementIt, reverseRSEIt);
                ++reverseRSEIt;
            }
            else
                ++selectedElementIt;
        }
    }
}

void CDataGenerator::CreateFilesAndReplicas(const TickType now)
{
    const std::uint32_t totalFilesToGen = GetRandomNumFilesToGenerate();

    float numReplicaRatio = mNumReplicaRatio.empty() ? 1 : mNumReplicaRatio[0];
    std::uint32_t numFilesToGen = static_cast<std::uint32_t>(totalFilesToGen * numReplicaRatio);
    CreateFilesAndReplicas(numFilesToGen, 1, now);

    for(std::size_t i=1; i<mNumReplicaRatio.size(); ++i)
    {
        numFilesToGen = static_cast<std::uint32_t>(totalFilesToGen * mNumReplicaRatio[i]);
        CreateFilesAndReplicas(numFilesToGen, i+1, now);
    }
}

void CDataGenerator::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    CreateFilesAndReplicas(now);

    mNextCallTick = now + mTickFreq;
}



CReaperCaller::CReaperCaller(CRucio *rucio, const TickType tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mRucio(rucio),
      mTickFreq(tickFreq)
{}

void CReaperCaller::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    mRucio->RunReaper(now);

    mNextCallTick = now + mTickFreq;
}



CBillingGenerator::CBillingGenerator(IBaseSim* sim, const TickType tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTickFreq(tickFreq)
{
    mCloudBillInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Bills(cloudName, month, bill) FROM STDIN with(FORMAT csv);", 3, '?');
}

void CBillingGenerator::OnUpdate(const TickType now)
{
    std::stringstream summary;

    std::unique_ptr<IInsertValuesContainer> billInsertQueries = mCloudBillInsertQuery->CreateValuesContainer(3 * mSim->mClouds.size());

    const std::uint32_t month = static_cast<std::uint32_t>(SECONDS_TO_MONTHS(now));

    const std::string caption = std::string(10, '=') + " Monthly Summary " + std::string(10, '=');

    summary << std::fixed << std::setprecision(2);

    summary << std::endl;
    summary << std::string(caption.length(), '=') << std::endl;
    summary << caption << std::endl;
    summary << std::string(caption.length(), '=') << std::endl;

    for(const std::unique_ptr<IBaseCloud>& cloud : mSim->mClouds)
    {
        std::string bill = cloud->ProcessBilling(now)->ToString();
        summary << std::endl;
        summary<<cloud->GetName()<<" - Billing for Month "<< month <<":\n";
        summary << bill;
        billInsertQueries->AddValue(cloud->GetName());
        billInsertQueries->AddValue(month);
        billInsertQueries->AddValue(std::move(bill));
    }

    summary << std::string(caption.length(), '=') << std::endl;
    std::cout << summary.str() << std::endl;

    COutput::GetRef().QueueInserts(std::move(billInsertQueries));

    mNextCallTick = now + mTickFreq;
}



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
                                        const TickType startAt)
    : mSrcReplica(srcReplica),
      mDstReplica(dstReplica),
      mNetworkLink(networkLink),
      mQueuedAt(queuedAt),
      mStartAt(startAt)
{}

CTransferManager::CTransferManager(const TickType tickFreq, const TickType startTick)
    : CBaseTransferManager(startTick),
      mTickFreq(tickFreq)
{
    mActiveTransfers.reserve(1024*1024);
    mQueuedTransfers.reserve(1024 * 1024);
    mOutputTransferInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Transfers(id, srcStorageElementId, dstStorageElementId, fileId, srcReplicaId, dstReplicaId, queuedAt, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 10, '?');
}

void CTransferManager::CreateTransfer(std::shared_ptr<SReplica> srcReplica, std::shared_ptr<SReplica> dstReplica, const TickType now)
{
    assert(mQueuedTransfers.size() < mQueuedTransfers.capacity());

    CStorageElement* const srcStorageElement = srcReplica->GetStorageElement();
    CNetworkLink* const networkLink = srcStorageElement->GetNetworkLink( dstReplica->GetStorageElement() );

    networkLink->mNumActiveTransfers += 1;
    srcStorageElement->OnOperation(CStorageElement::GET);
    mQueuedTransfers.emplace_back(srcReplica, dstReplica, networkLink, now, now);
}

void CTransferManager::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

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



CWavedTransferNumGen::CWavedTransferNumGen(const double softmaxScale, const double softmaxOffset, const std::uint32_t samplingFreq, const double baseFreq)
    : mSoftmaxScale(softmaxScale),
      mSoftmaxOffset(softmaxOffset),
      mAlpha(1.0/samplingFreq * PI/180.0 * baseFreq)
{}

auto CWavedTransferNumGen::GetNumToCreate(RNGEngineType& rngEngine, std::uint32_t numActive, const TickType now) -> std::uint32_t
{
    const double softmax = (std::cos(now * mAlpha) * mSoftmaxScale + mSoftmaxOffset) * (1.0 + mSoftmaxRNG(rngEngine) * 0.02);
    const double diffSoftmaxActive = softmax - numActive;
    if(diffSoftmaxActive < 0.5)
        return 0;
    return static_cast<std::uint32_t>(std::pow(diffSoftmaxActive, abs(mPeakinessRNG(rngEngine))));
}



CSimpleTransferGen::CSimpleTransferGen(IBaseSim* sim,
                                       std::shared_ptr<CTransferManager> transferMgr,
                                       const TickType tickFreq,
                                       const TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq)
{}

void CSimpleTransferGen::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    assert(!mTransferGenInfo.empty());

    for(std::unique_ptr<STransferGenInfo>& info : mTransferGenInfo)
    {
        CNetworkLink* networkLink = info->mNetworkLink;
        CStorageElement* dstStorageElement = networkLink->GetDstStorageElement();
        std::vector<std::shared_ptr<SReplica>>& replicas = info->mReplicas;
        while(networkLink->mNumActiveTransfers < info->mMaxNumActive && !replicas.empty())
        {
            std::shared_ptr<SReplica>& srcReplica = replicas.back();
            std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(srcReplica->GetFile(), now);

            assert(srcReplica->GetStorageElement() == dstStorageElement);
            assert(newReplica);

            mTransferMgr->CreateTransfer(srcReplica, newReplica, now);
            replicas.pop_back();
        }
    }

    mNextCallTick = now + mTickFreq;
}



CUniformTransferGen::CUniformTransferGen(IBaseSim* sim,
                                         std::shared_ptr<CTransferManager> transferMgr,
                                         std::shared_ptr<CWavedTransferNumGen> transferNumGen,
                                         const TickType tickFreq,
                                         const TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq),
      mTransferNumGen(transferNumGen)
{
    mReplicaInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Replicas(id, fileId, storageElementId, createdAt, expiredAt, deletedAt) FROM STDIN with(FORMAT csv);", 6, '?');
}

void CUniformTransferGen::OnUpdate(const TickType now)
{
    assert(mSrcStorageElements.size() > 0);

    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    RNGEngineType& rngEngine = mSim->mRNGEngine;
    std::uniform_int_distribution<std::size_t> dstStorageElementRndChooser(0, mDstStorageElements.size()-1);

    const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
    const std::uint32_t numToCreate = mTransferNumGen->GetNumToCreate(rngEngine, numActive, now);
    const std::uint32_t numToCreatePerRSE = static_cast<std::uint32_t>( numToCreate/static_cast<double>(mSrcStorageElements.size()) );

    std::unique_ptr<IInsertValuesContainer> replicaInsertQueries = mReplicaInsertQuery->CreateValuesContainer(numToCreate * 2);

    std::uint32_t totalTransfersCreated = 0;
    for(CStorageElement* srcStorageElement : mSrcStorageElements)
    {
        std::uint32_t numCreated = 0;
        std::size_t numSrcReplicas = srcStorageElement->mReplicas.size();
        std::uniform_int_distribution<std::size_t> rngSampler(0, numSrcReplicas);
        while(numSrcReplicas > 0 && numCreated < numToCreatePerRSE)
        {
            const std::size_t idx = rngSampler(rngEngine) % numSrcReplicas;
            --numSrcReplicas;

            std::shared_ptr<SReplica>& curReplica = srcStorageElement->mReplicas[idx];
            if(curReplica->IsComplete())
            {
                std::shared_ptr<SFile> file = curReplica->GetFile();
                CStorageElement* const dstStorageElement = mDstStorageElements[dstStorageElementRndChooser(rngEngine)];
                std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(file, now);
                if(newReplica != nullptr)
                {
                    replicaInsertQueries->AddValue(newReplica->GetId());
                    replicaInsertQueries->AddValue(file->GetId());
                    replicaInsertQueries->AddValue(dstStorageElement->GetId());
                    replicaInsertQueries->AddValue(now);
                    replicaInsertQueries->AddValue(newReplica->mExpiresAt);
                    mTransferMgr->CreateTransfer(curReplica, newReplica, now);
                    ++numCreated;
                }
            }
            std::shared_ptr<SReplica>& lastReplica = srcStorageElement->mReplicas[numSrcReplicas];
            std::swap(curReplica->mIndexAtStorageElement, lastReplica->mIndexAtStorageElement);
            std::swap(curReplica, lastReplica);
        }
        totalTransfersCreated += numCreated;
    }

    COutput::GetRef().QueueInserts(std::move(replicaInsertQueries));

    mNextCallTick = now + mTickFreq;
}



CExponentialTransferGen::CExponentialTransferGen(IBaseSim* sim,
                                                 std::shared_ptr<CTransferManager> transferMgr,
                                                 std::shared_ptr<CWavedTransferNumGen> transferNumGen,
                                                 const TickType tickFreq,
                                                 const TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq),
      mTransferNumGen(transferNumGen)
{
    mReplicaInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Replicas(id, fileId, storageElementId, createdAt, expiredAt, deletedAt) FROM STDIN with(FORMAT csv);", 6, '?');
}

void CExponentialTransferGen::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    const std::size_t numSrcStorageElements = mSrcStorageElements.size();
    const std::size_t numDstStorageElements = mDstStorageElements.size();
    assert(numSrcStorageElements > 0 && numDstStorageElements > 0);

    RNGEngineType& rngEngine = mSim->mRNGEngine;
    std::exponential_distribution<double> dstStorageElementRndSelecter(0.25);

    const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
    const std::uint32_t numToCreate = mTransferNumGen->GetNumToCreate(rngEngine, numActive, now);

    std::unique_ptr<IInsertValuesContainer> replicaInsertQueries = mReplicaInsertQuery->CreateValuesContainer(numToCreate * 2);

    for(std::uint32_t totalTransfersCreated=0; totalTransfersCreated<numToCreate; ++totalTransfersCreated)
    {
        CStorageElement* const dstStorageElement = mDstStorageElements[static_cast<std::size_t>(dstStorageElementRndSelecter(rngEngine) * 2) % numDstStorageElements];
        bool wasTransferCreated = false;
        for(std::size_t numSrcStorageElementsTried = 0; numSrcStorageElementsTried < numSrcStorageElements; ++numSrcStorageElementsTried)
        {
            std::uniform_int_distribution<std::size_t> srcStorageElementRndSelecter(numSrcStorageElementsTried, numSrcStorageElements - 1);
            CStorageElement*& srcStorageElement = mSrcStorageElements[srcStorageElementRndSelecter(rngEngine)];
            const std::size_t numSrcReplicas = srcStorageElement->mReplicas.size();
            for(std::size_t numSrcReplciasTried = 0; numSrcReplciasTried < numSrcReplicas; ++numSrcReplciasTried)
            {
                std::uniform_int_distribution<std::size_t> srcReplicaRndSelecter(numSrcReplciasTried, numSrcReplicas - 1);
                std::shared_ptr<SReplica>& curReplica = srcStorageElement->mReplicas[srcReplicaRndSelecter(rngEngine)];
                if(curReplica->IsComplete())
                {
                    std::shared_ptr<SFile> file = curReplica->GetFile();
                    std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(file, now);
                    if(newReplica != nullptr)
                    {
                        replicaInsertQueries->AddValue(newReplica->GetId());
                        replicaInsertQueries->AddValue(file->GetId());
                        replicaInsertQueries->AddValue(dstStorageElement->GetId());
                        replicaInsertQueries->AddValue(now);
                        replicaInsertQueries->AddValue(newReplica->mExpiresAt);
                        mTransferMgr->CreateTransfer(curReplica, newReplica, now);
                        wasTransferCreated = true;
                        break;
                    }
                }
                std::shared_ptr<SReplica>& firstReplica = srcStorageElement->mReplicas.front();
                std::swap(curReplica->mIndexAtStorageElement, firstReplica->mIndexAtStorageElement);
                std::swap(curReplica, firstReplica);
            }
            if(wasTransferCreated)
                break;
            std::swap(srcStorageElement, mSrcStorageElements.front());
        }
    }

    COutput::GetRef().QueueInserts(std::move(replicaInsertQueries));

    mNextCallTick = now + mTickFreq;
}



CSrcPrioTransferGen::CSrcPrioTransferGen(IBaseSim* sim,
                                         std::shared_ptr<CTransferManager> transferMgr,
                                         std::shared_ptr<CWavedTransferNumGen> transferNumGen,
                                         const TickType tickFreq,
                                         const TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq),
      mTransferNumGen(transferNumGen)
{
    mReplicaInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Replicas(id, fileId, storageElementId, createdAt, expiredAt, deletedAt) FROM STDIN with(FORMAT csv);", 6, '?');
}

void CSrcPrioTransferGen::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    const std::vector<std::shared_ptr<SFile>>& allFiles = mSim->mRucio->mFiles;
    const std::size_t numDstStorageElements = mDstStorageElements.size();
    assert(allFiles.size() > 0 && numDstStorageElements > 0);

    RNGEngineType& rngEngine = mSim->mRNGEngine;
    std::exponential_distribution<double> dstStorageElementRndSelecter(0.25);
    std::uniform_int_distribution<std::size_t> fileRndSelector(0, allFiles.size() - 1);

    const std::uint32_t numActive = static_cast<std::uint32_t>(mTransferMgr->GetNumActiveTransfers());
    const std::uint32_t numToCreate = mTransferNumGen->GetNumToCreate(rngEngine, numActive, now);

    std::unique_ptr<IInsertValuesContainer> replicaInsertQueries = mReplicaInsertQuery->CreateValuesContainer(numToCreate * 2);
    std::uint32_t flexCreationLimit = numToCreate;
    for(std::uint32_t totalTransfersCreated=0; totalTransfersCreated< flexCreationLimit; ++totalTransfersCreated)
    {
        CStorageElement* const dstStorageElement = mDstStorageElements[static_cast<std::size_t>(dstStorageElementRndSelecter(rngEngine) * 2) % numDstStorageElements];
        std::shared_ptr<SFile> fileToTransfer = allFiles[fileRndSelector(rngEngine)];

        for(std::uint32_t i = 0; i < 5 && fileToTransfer->mReplicas.empty() && fileToTransfer->mExpiresAt < (now + 100); ++i)
            fileToTransfer = allFiles[fileRndSelector(rngEngine)];

        const std::vector<std::shared_ptr<SReplica>>& replicas = fileToTransfer->mReplicas;
        if(replicas.empty())
        {
            flexCreationLimit += 1;
            continue;
        }

        std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(fileToTransfer, now);
        if(newReplica != nullptr)
        {
            newReplica->mExpiresAt = now + SECONDS_PER_DAY;
            int minPrio = std::numeric_limits<int>::max();
            std::vector<std::shared_ptr<SReplica>> bestSrcReplicas;
            for(const std::shared_ptr<SReplica>& replica : replicas)
            {
                if(!replica->IsComplete())
                    continue; // at least the newly created one will be skipped

                const auto result = mSrcStorageElementIdToPrio.find(replica->GetStorageElement()->GetId());
                if(result != mSrcStorageElementIdToPrio.cend())
                {
                    if(result->second < minPrio)
                    {
                        minPrio = result->second;
                        bestSrcReplicas.clear();
                        bestSrcReplicas.emplace_back(replica);
                    }
                    else if(result->second == minPrio)
                        bestSrcReplicas.emplace_back(replica);
                }
            }

            if(bestSrcReplicas.empty())
            {
                flexCreationLimit += 1;
                continue;
            }

            std::shared_ptr<SReplica> bestSrcReplica = bestSrcReplicas[0];
            if (minPrio > 0)
            {
                double minWeight = std::numeric_limits<double>::max();
                for (std::shared_ptr<SReplica>& replica : bestSrcReplicas)
                {
                    double w = 0; //todo
                    if (w < minWeight)
                    {
                        minWeight = w;
                        bestSrcReplica = replica;
                    }
                }
            }
            replicaInsertQueries->AddValue(newReplica->GetId());
            replicaInsertQueries->AddValue(fileToTransfer->GetId());
            replicaInsertQueries->AddValue(dstStorageElement->GetId());
            replicaInsertQueries->AddValue(now);
            replicaInsertQueries->AddValue(newReplica->mExpiresAt);

            mTransferMgr->CreateTransfer(bestSrcReplica, newReplica, now);
        }
        else
        {
            //replica already exists
        }
    }

    COutput::GetRef().QueueInserts(std::move(replicaInsertQueries));

    mNextCallTick = now + mTickFreq;
}



CJobSlotTransferGen::CJobSlotTransferGen(IBaseSim* sim,
                                         std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                                         const TickType tickFreq,
                                         const TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq)
{
    mReplicaInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Replicas(id, fileId, storageElementId, createdAt, expiredAt, deletedAt) FROM STDIN with(FORMAT csv);", 6, '?');
}

void CJobSlotTransferGen::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    const std::vector<std::shared_ptr<SFile>>& allFiles = mSim->mRucio->mFiles;
    assert(allFiles.size() > 0);

    RNGEngineType& rngEngine = mSim->mRNGEngine;
    std::uniform_int_distribution<std::size_t> fileRndSelector(0, allFiles.size() - 1);


    std::unique_ptr<IInsertValuesContainer> replicaInsertQueries = mReplicaInsertQuery->CreateValuesContainer(512);
    for(auto& dstInfo : mDstInfo)
    {
        CStorageElement* const dstStorageElement = dstInfo.first;
        SJobSlotInfo& jobSlotInfo = dstInfo.second;

        auto& schedule = jobSlotInfo.mSchedule;
        const std::uint32_t numMaxSlots = jobSlotInfo.mNumMaxSlots;
        std::uint32_t usedSlots = 0;
        for(std::size_t idx=0; idx<schedule.size();)
        {
            if(schedule[idx].first <= now)
            {
                schedule[idx] = std::move(schedule.back());
                schedule.pop_back();
                continue;
            }
            usedSlots += schedule[idx].second;
            idx += 1;
        }

        assert(numMaxSlots >= usedSlots);

        // todo: consider mTickFreq
        std::uint32_t flexCreationLimit = std::min(numMaxSlots - usedSlots, std::uint32_t(1 + (0.01 * numMaxSlots)));
        std::pair<TickType, std::uint32_t> newJobs = std::make_pair(now+900, 0);
        for(std::uint32_t totalTransfersCreated=0; totalTransfersCreated<flexCreationLimit; ++totalTransfersCreated)
        {
            std::shared_ptr<SFile> fileToTransfer = allFiles[fileRndSelector(rngEngine)];

            for(std::uint32_t i = 0; i < 10 && fileToTransfer->mReplicas.empty() && fileToTransfer->mExpiresAt < (now + 100); ++i)
                fileToTransfer = allFiles[fileRndSelector(rngEngine)];

            const std::vector<std::shared_ptr<SReplica>>& replicas = fileToTransfer->mReplicas;
            if(replicas.empty())
            {
                flexCreationLimit += 1;
                continue;
            }

            std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(fileToTransfer, now);
            if(newReplica != nullptr)
            {
                newReplica->mExpiresAt = now + SECONDS_PER_DAY;
                int minPrio = std::numeric_limits<int>::max();
                std::vector<std::shared_ptr<SReplica>> bestSrcReplicas;
                for(const std::shared_ptr<SReplica>& replica : replicas)
                {
                    if(!replica->IsComplete())
                        continue; // at least the newly created one will be skipped

                    const auto result = mSrcStorageElementIdToPrio.find(replica->GetStorageElement()->GetId());
                    if(result != mSrcStorageElementIdToPrio.cend())
                    {
                        if(result->second < minPrio)
                        {
                            minPrio = result->second;
                            bestSrcReplicas.clear();
                            bestSrcReplicas.emplace_back(replica);
                        }
                        else if(result->second == minPrio)
                            bestSrcReplicas.emplace_back(replica);
                    }
                }

                if(bestSrcReplicas.empty())
                {
                    flexCreationLimit += 1;
                    continue;
                }

                std::shared_ptr<SReplica> bestSrcReplica = bestSrcReplicas[0];
                if (minPrio > 0)
                {
                    double minWeight = std::numeric_limits<double>::max();
                    for (std::shared_ptr<SReplica>& replica : bestSrcReplicas)
                    {
                        double w = 0; //todo
                        if (w < minWeight)
                        {
                            minWeight = w;
                            bestSrcReplica = replica;
                        }
                    }
                }
                replicaInsertQueries->AddValue(newReplica->GetId());
                replicaInsertQueries->AddValue(fileToTransfer->GetId());
                replicaInsertQueries->AddValue(dstStorageElement->GetId());
                replicaInsertQueries->AddValue(now);
                replicaInsertQueries->AddValue(newReplica->mExpiresAt);

                mTransferMgr->CreateTransfer(bestSrcReplica, newReplica, now, 0, 60); //TODO: fix delay
                newJobs.second += 1;
            }
            else
            {
                //replica already exists
            }
        }
        if(newJobs.second > 0)
            schedule.push_back(newJobs);
    }

    COutput::GetRef().QueueInserts(std::move(replicaInsertQueries));

    mNextCallTick = now + mTickFreq;
}



CBaseOnDeletionInsert::CBaseOnDeletionInsert()
{
    mFileInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Files(id, createdAt, expiredAt, filesize) FROM STDIN with(FORMAT csv);", 4, '?');
    mReplicaInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Replicas(id, fileId, storageElementId, createdAt, expiredAt) FROM STDIN with(FORMAT csv);", 5, '?');
}

void CBaseOnDeletionInsert::OnFileCreated(const TickType now, std::shared_ptr<SFile> file)
{
    (void)now;
    (void)file;
}

void CBaseOnDeletionInsert::OnFilesDeleted(const TickType now, const std::vector<std::weak_ptr<SFile>>& deletedFiles)
{
    (void)now;
    std::unique_ptr<IInsertValuesContainer> fileInsertQueries = mFileInsertQuery->CreateValuesContainer(deletedFiles.size() * 4);
    for(const std::weak_ptr<SFile>& weakFile : deletedFiles)
    {
        std::shared_ptr<SFile> file = weakFile.lock();

        assert(file);

        fileInsertQueries->AddValue(file->GetId());
        fileInsertQueries->AddValue(file->GetCreatedAt());
        fileInsertQueries->AddValue(file->mExpiresAt);
        fileInsertQueries->AddValue(file->GetSize());
    }
    COutput::GetRef().QueueInserts(std::move(fileInsertQueries));
}

void CBaseOnDeletionInsert::OnReplicaCreated(const TickType now, std::shared_ptr<SReplica> replica)
{
    (void)now;
    (void)replica;
}

void CBaseOnDeletionInsert::OnReplicasDeleted(const TickType now, const std::vector<std::weak_ptr<SReplica>>& deletedReplicas)
{
    (void)now;
    std::unique_ptr<IInsertValuesContainer> replicaInsertQueries = mReplicaInsertQuery->CreateValuesContainer(deletedReplicas.size() * 5);
    for(const std::weak_ptr<SReplica>& weakReplica : deletedReplicas)
    {
        std::shared_ptr<SReplica> replica = weakReplica.lock();

        assert(replica);

        replicaInsertQueries->AddValue(replica->GetId());
        replicaInsertQueries->AddValue(replica->GetFile()->GetId());
        replicaInsertQueries->AddValue(replica->GetStorageElement()->GetId());
        replicaInsertQueries->AddValue(replica->GetCreatedAt());
        replicaInsertQueries->AddValue(replica->mExpiresAt);
    }
    COutput::GetRef().QueueInserts(std::move(replicaInsertQueries));
}




CCachedSrcTransferGen::CCachedSrcTransferGen(IBaseSim* sim,
                                             std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                                             const std::size_t numPerDay,
                                             const TickType defaultReplicaLifetime,
                                             const TickType tickFreq,
                                             const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq),
      mNumPerDay(numPerDay),
      mDefaultReplicaLifetime(defaultReplicaLifetime)
{}

bool CCachedSrcTransferGen::ExistsFileAtStorageElement(const std::shared_ptr<SFile>& file, const CStorageElement* storageElement) const
{
    for(const std::shared_ptr<SReplica>& r : file->mReplicas)
    {
        if(r->GetStorageElement() == storageElement)
            return true;
    }
    return false;
}

void CCachedSrcTransferGen::ExpireReplica(CStorageElement* storageElement, const TickType now)
{
    const std::vector<std::shared_ptr<SReplica>>& replicas = storageElement->mReplicas;
    if(replicas.empty())
        return;
    auto replicasIt = replicas.begin();
    auto oldestReplicaIt = replicasIt;
    TickType oldestTime = (*replicasIt)->mExpiresAt;
    if((replicas.size()/mTickFreq) >= 50)
    {
        //to increase performance we just randomly pick 100 replicas
        //and expire the oldest one of these
        const std::size_t numSamples = static_cast<std::size_t>(replicas.size() * 0.05);
        std::uniform_int_distribution<std::size_t> replicaRndSelector(0, replicas.size() - 1);
        for(std::size_t i=0; i<numSamples; ++i)
        {
            replicasIt = replicas.begin() + replicaRndSelector(mSim->mRNGEngine);
            if((*replicasIt)->mExpiresAt < oldestTime)
            {
                oldestTime = (*replicasIt)->mExpiresAt;
                oldestReplicaIt = replicasIt;
            }
        }
    }
    else
    {
        while(replicasIt != replicas.end())
        {
            if((*replicasIt)->mExpiresAt < oldestTime)
            {
                oldestTime = (*replicasIt)->mExpiresAt;
                oldestReplicaIt = replicasIt;
            }
            replicasIt++;
        }
    }
    std::vector<std::shared_ptr<SReplica>> expiredReplicas;
    (*oldestReplicaIt)->mExpiresAt = now;
    (*oldestReplicaIt)->GetFile()->ExtractExpiredReplicas(now, expiredReplicas);

    std::vector<std::weak_ptr<SReplica>> expiredWeakReplicas;
    expiredWeakReplicas.reserve(expiredReplicas.size());
    for(std::shared_ptr<SReplica>& r : expiredReplicas)
        expiredWeakReplicas.emplace_back(r);
    for(std::weak_ptr<IReplicaActionListener> replicaListener : mSim->mRucio->mReplicaActionListeners)
    {
        if(std::shared_ptr<IReplicaActionListener> listener = replicaListener.lock())
            listener->OnReplicasDeleted(now, expiredWeakReplicas);
    }
}

void CCachedSrcTransferGen::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    RNGEngineType& rngEngine = mSim->mRNGEngine;

    const std::size_t numTotalTransfersPerUpdate = static_cast<std::size_t>((mNumPerDay * mTickFreq) / SECONDS_PER_DAY);
    for(auto it=mRatiosAndFilesPerAccessCount.rbegin(); it!=mRatiosAndFilesPerAccessCount.rend(); ++it)
    {
        const std::size_t numTransfersToCreate = 1 + static_cast<std::size_t>(numTotalTransfersPerUpdate * it->first);
        std::vector<std::weak_ptr<SFile>>& fileVec = it->second;

        if(fileVec.empty())
            continue;

        // create transfers per dst storage element
        for(CStorageElement* dstStorageElement : mDstStorageElements)
        {
            // create numTransfersToCreate many transfers
            for(std::size_t numTransfersCreated=0; numTransfersCreated<numTransfersToCreate; ++numTransfersCreated)
            {
                const std::size_t numFileSelectRetries = std::min<std::size_t>(fileVec.size(), 10);
                std::uniform_int_distribution<std::size_t> fileRndSelector(0, fileVec.size() - 1);

                //try to find a file that is not already on the dst storage element
                std::size_t fileIdx;
                std::shared_ptr<SFile> fileToTransfer = nullptr;
                for(std::size_t i=0; (i<numFileSelectRetries) && !fileToTransfer ; ++i)
                {
                    fileIdx = fileRndSelector(rngEngine);
                    fileToTransfer = fileVec[fileIdx].lock();
                    if(!fileToTransfer)
                    {
                        std::swap(fileVec[fileIdx], fileVec.back());
                        fileVec.pop_back();
                        continue;
                    }
                    if(fileToTransfer->mReplicas.empty() || ExistsFileAtStorageElement(fileToTransfer, dstStorageElement))
                        fileToTransfer = nullptr;
                }

                if(!fileToTransfer)
                    continue;

                //find best src replica
                std::shared_ptr<SReplica> bestSrcReplica = nullptr;

                //check caches first
                if(!mCacheElements.empty())
                {
                    for(const SCacheElementInfo& cacheElement : mCacheElements)
                    {
                        for (const std::shared_ptr<SReplica>& r : fileToTransfer->mReplicas)
                        {
                            if (r->GetStorageElement() == cacheElement.mStorageElement)
                            {
                                bestSrcReplica = r;
                                break;
                            }
                        }
                        if (bestSrcReplica)
                            break;
                    }
                }

                if(!bestSrcReplica)
                {
                    double minWeight = 0;
                    for(std::shared_ptr<SReplica>& srcReplica : fileToTransfer->mReplicas)
                    {
                        if(!srcReplica->IsComplete())
                            continue;

                        const double weight = 0; // todo
                        if(!bestSrcReplica || weight < minWeight)
                        {
                            bestSrcReplica = srcReplica;
                            minWeight = weight;
                        }
                    }

                    if(!bestSrcReplica)
                        continue;

                    //handle cache miss here so that we didnt have to create the new cache replica before
                    //dont create transfer to cache if element was accessed the last time
                    if(!mCacheElements.empty() && (it != mRatiosAndFilesPerAccessCount.rbegin()))
                    {
                        //todo: randomise cache element selection?
                        CStorageElement* cacheStorageElement = mCacheElements[0].mStorageElement;
                        if(cacheStorageElement->mReplicas.size() >= mCacheElements[0].mCacheSize)
                            ExpireReplica(cacheStorageElement, now);

                        std::shared_ptr<SReplica> newCacheReplica = cacheStorageElement->CreateReplica(fileToTransfer, now);
                        assert(newCacheReplica);
                        newCacheReplica->mExpiresAt = now + mCacheElements[0].mDefaultReplicaLifetime;

                        mTransferMgr->CreateTransfer(bestSrcReplica, newCacheReplica, now, 0, 60); //TODO: fix delay
                    }
                }
                else if(!bestSrcReplica->IsComplete())
                {
                    //replica is already going to be transferred to the cache
                    continue;
                }
                else if(it == mRatiosAndFilesPerAccessCount.rbegin())
                {
                    //cache hit for file that will never be accessed again
                    //->delete after transfer
                }

                std::shared_ptr<SReplica> newReplica = dstStorageElement->CreateReplica(fileToTransfer, now);
                assert(newReplica);
                newReplica->mExpiresAt = now + mDefaultReplicaLifetime;

                mTransferMgr->CreateTransfer(bestSrcReplica, newReplica, now, 0, 60); //TODO: fix delay

                std::swap(fileVec[fileIdx], fileVec.back());
                fileVec.pop_back();
                if (it != mRatiosAndFilesPerAccessCount.rbegin())
                    (it-1)->second.push_back(fileToTransfer);
            }
        }
    }

    mNextCallTick = now + mTickFreq;
}

void CCachedSrcTransferGen::Shutdown(const TickType now)
{
    std::vector<std::weak_ptr<SFile>> files;
    files.reserve(mSim->mRucio->mFiles.size());
    std::vector<std::weak_ptr<SReplica>> replicas;
    replicas.reserve(files.size() * 4);
    for(std::shared_ptr<SFile>& file : mSim->mRucio->mFiles)
    {
        files.emplace_back(file);
        for(std::shared_ptr<SReplica>& replica : file->mReplicas)
            replicas.emplace_back(replica);
    }
    OnFilesDeleted(now, files);
    OnReplicasDeleted(now, replicas);
}

void CCachedSrcTransferGen::OnFileCreated(const TickType now, std::shared_ptr<SFile> file)
{
    CBaseOnDeletionInsert::OnFileCreated(now, file);
    mRatiosAndFilesPerAccessCount[0].second.emplace_back(file);
}



CHeartbeat::CHeartbeat(IBaseSim* sim, const TickType tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTickFreq(tickFreq)
{
    mTimeLastUpdate = std::chrono::high_resolution_clock::now();
}

void CHeartbeat::OnUpdate(const TickType now)
{
    auto curRealtime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> timeDiff = curRealtime - mTimeLastUpdate;
    mUpdateDurationSummed += timeDiff;
    mTimeLastUpdate = curRealtime;

    std::stringstream statusOutput;
    statusOutput << std::fixed << std::setprecision(2);

    statusOutput << "[" << std::setw(6) << static_cast<TickType>(now / 1000.0) << "k]: ";
    statusOutput << "Runtime: " << mUpdateDurationSummed.count() << "s; ";
    statusOutput << "numFiles: " << static_cast<std::size_t>(mSim->mRucio->mFiles.size() / 1000.0) << "k" << std::endl;
    
    statusOutput << "Transfer stats:" << std::endl;
    for (std::shared_ptr<CBaseTransferManager>& transferManager : mTransferManagers)
    {
        std::uint64_t sumNumTransfers = transferManager->GetNumActiveTransfers() + transferManager->mNumCompletedTransfers + transferManager->mNumFailedTransfers;
        statusOutput << transferManager->mName << std::endl;

        statusOutput << "  avg duration: ";
        if (sumNumTransfers > 0)
            statusOutput << transferManager->mSummedTransferDuration / sumNumTransfers;
        else
            statusOutput << "-";
        statusOutput << std::endl;

        statusOutput << "        active: " << transferManager->GetNumActiveTransfers() << std::endl;
        statusOutput << "          done: " << transferManager->mNumCompletedTransfers << std::endl;
        statusOutput << "        failed: " << transferManager->mNumFailedTransfers << std::endl;
        
        transferManager->mSummedTransferDuration = 0;
        transferManager->mNumCompletedTransfers = 0;
        transferManager->mNumFailedTransfers = 0;
    }


    std::size_t maxW = 0;
    for (auto it : mProccessDurations)
        if (it.first.size() > maxW)
            maxW = it.first.size();

    statusOutput << "Sim stats:" << std::endl;
    statusOutput << "  " << std::setw(maxW) << "Duration: " << std::setw(6) << timeDiff.count() << "s\n";
    for(auto it : mProccessDurations)
    {
        statusOutput << "  " << std::setw(maxW) << it.first;
        statusOutput << ": " << std::setw(6) << it.second->count();
        statusOutput << "s ("<< std::setw(5) << (it.second->count() / timeDiff.count()) * 100 << "%)\n";
        *(it.second) = std::chrono::duration<double>::zero();
    }
    std::cout << statusOutput.str() << std::endl;

    mNextCallTick = now + mTickFreq;
}
