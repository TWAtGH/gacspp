#include <cassert>
#include <iostream>

#include "TransferGenerators.hpp"
#include "TransferManager.hpp"

#include "common/utils.hpp"

#include "infrastructure/CRucio.hpp"
#include "infrastructure/CNetworkLink.hpp"
#include "infrastructure/CStorageElement.hpp"
#include "infrastructure/IActionListener.hpp"
#include "infrastructure/SFile.hpp"

#include "sim/IBaseSim.hpp"

#include "output/COutput.hpp"


CBaseOnDeletionInsert::CBaseOnDeletionInsert()
{
    mFileInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Files(id, createdAt, expiredAt, filesize, popularity) FROM STDIN with(FORMAT csv);", 5, '?');
    mReplicaInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Replicas(id, fileId, storageElementId, createdAt, expiredAt) FROM STDIN with(FORMAT csv);", 5, '?');
}

void CBaseOnDeletionInsert::PostCreateFile(SFile* file, TickType now)
{
    (void)file;
    (void)now;
}

void CBaseOnDeletionInsert::PostCreateReplica(SReplica* replica, TickType now)
{
    (void)replica;
    (void)now;
}

void CBaseOnDeletionInsert::AddFileDelete(SFile* file)
{
    mFileValueContainer->AddValue(file->GetId());
    mFileValueContainer->AddValue(file->GetCreatedAt());
    mFileValueContainer->AddValue(file->mExpiresAt);
    mFileValueContainer->AddValue(file->GetSize());
    mFileValueContainer->AddValue(file->mPopularity);
}

void CBaseOnDeletionInsert::AddReplicaDelete(SReplica* replica)
{
    mReplicaValueContainer->AddValue(replica->GetId());
    mReplicaValueContainer->AddValue(replica->GetFile()->GetId());
    mReplicaValueContainer->AddValue(replica->GetStorageElement()->GetId());
    mReplicaValueContainer->AddValue(replica->GetCreatedAt());
    mReplicaValueContainer->AddValue(replica->mExpiresAt);
}

void CBaseOnDeletionInsert::PreRemoveFile(SFile* file, TickType now)
{
    (void)now;
    std::unique_ptr<IInsertValuesContainer> mFileValueContainer = mFileInsertQuery->CreateValuesContainer();

    AddFileDelete(file);

    COutput::GetRef().QueueInserts(std::move(mFileValueContainer));
}

void CBaseOnDeletionInsert::PreRemoveReplica(SReplica* replica, TickType now)
{
    (void)now;
    std::unique_ptr<IInsertValuesContainer> mReplicaValueContainer = mReplicaInsertQuery->CreateValuesContainer();

    AddReplicaDelete(replica);

    COutput::GetRef().QueueInserts(std::move(mReplicaValueContainer));
}


CBufferedOnDeletionInsert::~CBufferedOnDeletionInsert()
{
    if(mFileValueContainer)
        FlushFileDeletes();
    if(mReplicaValueContainer)
        FlushReplicaDeletes();
}

void CBufferedOnDeletionInsert::FlushFileDeletes()
{
    if(!mFileValueContainer->IsEmpty())
        COutput::GetRef().QueueInserts(std::move(mFileValueContainer));
}

void CBufferedOnDeletionInsert::FlushReplicaDeletes()
{
    if(!mReplicaValueContainer->IsEmpty())
        COutput::GetRef().QueueInserts(std::move(mReplicaValueContainer));
}

void CBufferedOnDeletionInsert::PreRemoveFile(SFile* file, TickType now)
{
    (void)now;
    constexpr std::size_t valueBufSize = 5000 * 4;
    if(!mFileValueContainer)
        mFileValueContainer = mFileInsertQuery->CreateValuesContainer(valueBufSize);

    AddFileDelete(file);

    if(mFileValueContainer->GetSize() >= valueBufSize)
        FlushFileDeletes();
}

void CBufferedOnDeletionInsert::PreRemoveReplica(SReplica* replica, TickType now)
{
    (void)now;
    constexpr std::size_t valueBufSize = 5000 * 5;
    if(!mReplicaValueContainer)
        mReplicaValueContainer = mReplicaInsertQuery->CreateValuesContainer(valueBufSize);

    AddReplicaDelete(replica);

    if(mReplicaValueContainer->GetSize() >= valueBufSize)
        FlushReplicaDeletes();
}



CCloudBufferTransferGen::CCloudBufferTransferGen(IBaseSim* sim,
                                                 std::shared_ptr<CTransferManager> transferMgr,
                                                 TickType tickFreq,
                                                 TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(std::move(transferMgr)),
      mTickFreq(tickFreq)
{}

CCloudBufferTransferGen::~CCloudBufferTransferGen()
{
    for (std::unique_ptr<STransferGenInfo>& info : mTransferGenInfo)
    {
        auto listenerIt = info->mPrimaryLink->GetSrcStorageElement()->mActionListener.begin();
        auto listenerEnd = info->mPrimaryLink->GetSrcStorageElement()->mActionListener.end();
        for (; listenerIt != listenerEnd; ++listenerIt)
        {
            if ((*listenerIt) == this)
                break;
        }
        if (listenerIt != listenerEnd)
            info->mPrimaryLink->GetSrcStorageElement()->mActionListener.erase(listenerIt);
    }
}

void CCloudBufferTransferGen::PostCreateReplica(SReplica* replica, TickType now)
{
    (void)now;
    for(std::unique_ptr<STransferGenInfo>& info : mTransferGenInfo)
    {
        if(replica->GetStorageElement() == info->mPrimaryLink->GetSrcStorageElement())
        {
            const std::uint32_t numReusages = info->mReusageNumGen->GetValue(mSim->mRNGEngine);
            replica->GetFile()->mPopularity = numReusages;
            std::forward_list<SReplica*>& replicas = info->mReplicas;
            auto prev = replicas.before_begin();
            auto cur = replicas.begin();
            while(cur != replicas.end())
            {
                if((*cur)->GetFile()->mPopularity >= numReusages)
                    break;

                prev = cur;
                cur++;
            }
            replicas.insert_after(prev, replica);
            return;
        }
    }
}

void CCloudBufferTransferGen::PreRemoveReplica(SReplica* replica, TickType now)
{
    //shouldnt be called
    (void)now;
    for (std::unique_ptr<STransferGenInfo>& info : mTransferGenInfo)
    {
        if (replica->GetStorageElement() == info->mPrimaryLink->GetSrcStorageElement())
        {
            info->mReplicas.remove(replica);
            break;
        }
    }
}

void CCloudBufferTransferGen::OnUpdate(TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    assert(!mTransferGenInfo.empty());

    for(std::unique_ptr<STransferGenInfo>& info : mTransferGenInfo)
    {
        CNetworkLink* networkLink = info->mPrimaryLink;
        CStorageElement* dstStorageElement = networkLink->GetDstStorageElement();
        std::forward_list<SReplica*>& replicas = info->mReplicas;
        auto prev = replicas.before_begin();
        auto cur = replicas.begin();
        while(cur != replicas.end())
        {
            SReplica* srcReplica = (*cur);
            if(!srcReplica->IsComplete())
            {
                //handle this case better
                //replicas with high re-usage number should still be preferred for primary storage
                //even if they are not complete yet
                prev = cur;
                cur++;
                continue;
            }

            if(networkLink->mNumActiveTransfers < networkLink->mMaxNumActiveTransfers)
            {
                SFile* file = srcReplica->GetFile();
                SReplica* newReplica = dstStorageElement->CreateReplica(file, now);
                if(!newReplica && info->mSecondaryLink)
                {
                    networkLink = info->mSecondaryLink;
                    if(networkLink->mNumActiveTransfers < networkLink->mMaxNumActiveTransfers)
                    {
                        dstStorageElement = networkLink->GetDstStorageElement();
                        newReplica = dstStorageElement->CreateReplica(file, now);
                    }
                }

                if(newReplica)
                {
                    mTransferMgr->CreateTransfer(srcReplica, newReplica, now, mDeleteSrcReplica);
                    cur = replicas.erase_after(prev);
                    continue; //same idx again
                }
            }

            break;
        }
    }

    mNextCallTick = now + mTickFreq;
}



CJobIOTransferGen::CJobIOTransferGen(IBaseSim* sim,
                                    std::shared_ptr<CTransferManager> transferMgr,
                                    TickType tickFreq,
                                    TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq)
{
    mTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Traces(id, jobId, storageElementId, fileId, replicaId, type, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 9, '?');
}

void CJobIOTransferGen::OnUpdate(TickType now)
{
    assert(now >= mLastUpdateTime);

    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    const TickType tDelta = now - mLastUpdateTime;
    mLastUpdateTime = now;
    
    std::unique_ptr<IInsertValuesContainer> traceInsertQueries = mTraceInsertQuery->CreateValuesContainer(800);
    RNGEngineType& rngEngine = mSim->mRNGEngine;
    
    for(SSiteInfo& siteInfo : mSiteInfos)
    {
        std::list<SJobInfo>& jobInfos = siteInfo.mJobInfos;
        CNetworkLink* diskToCPULink = siteInfo.mDiskToCPULink;
        CNetworkLink* cpuToOutputLink = siteInfo.mCPUToOutputLink;

        CStorageElement* diskSE = diskToCPULink->GetSrcStorageElement();
        CStorageElement* cpuSE = diskToCPULink->GetDstStorageElement();
        CStorageElement* outputSE = cpuToOutputLink->GetDstStorageElement();

        //should firstly update all mNumActiveTransfers and then calculate bytesDownloaded/uploaded
        const SpaceType bytesDownloaded = (diskToCPULink->mBandwidthBytesPerSecond / (double)(diskToCPULink->mNumActiveTransfers+1)) * tDelta;

        auto jobInfoIt = jobInfos.begin();
        while(jobInfoIt != jobInfos.end())
        {
            SFile* inputFile = jobInfoIt->mInputFile;
            std::vector<SReplica*>& outputReplicas = jobInfoIt->mOutputReplicas;
            if(jobInfoIt->mCurInputFileSize < inputFile->GetSize())
            {
                //still downloading input

                //update download
                if(jobInfoIt->mCurInputFileSize == 0)
                {
                    //download just started
                    jobInfoIt->mCurInputFileSize += 1;
                    jobInfoIt->mStartedAt = now;
                    diskToCPULink->mNumActiveTransfers += 1;
                }

                SpaceType newSize = jobInfoIt->mCurInputFileSize + bytesDownloaded;
                if(newSize >= inputFile->GetSize())
                {
                    //download completed
                    newSize = inputFile->GetSize();
                    diskToCPULink->mUsedTraffic += newSize - jobInfoIt->mCurInputFileSize;
                    diskToCPULink->mNumActiveTransfers -= 1;
                    diskToCPULink->mNumDoneTransfers += 1;

                    traceInsertQueries->AddValue(GetNewId());
                    traceInsertQueries->AddValue(jobInfoIt->mJobId);
                    traceInsertQueries->AddValue(diskSE->GetId());
                    traceInsertQueries->AddValue(inputFile->GetId());
                    //ensure src replica stays available
                    IdType srcReplicaId = inputFile->GetReplicaByStorageElement(diskSE)->GetId();
                    traceInsertQueries->AddValue(srcReplicaId);
                    traceInsertQueries->AddValue(0);
                    traceInsertQueries->AddValue(jobInfoIt->mStartedAt);
                    traceInsertQueries->AddValue(now);
                    traceInsertQueries->AddValue(inputFile->GetSize());

                    jobInfoIt->mStartedAt = now;
                    jobInfoIt->mFinishedAt = now + siteInfo.mJobDurationGen->GetValue(rngEngine);
                }
                else
                    diskToCPULink->mUsedTraffic += bytesDownloaded;
                
                jobInfoIt->mCurInputFileSize = newSize;
            }
            else if(outputReplicas.empty() && (now >= jobInfoIt->mFinishedAt))
            {
                //no upload created yet but job finished

                //create upload
                //todo: consider cpuToOutputLink->mMaxNumActiveTransfers
                std::size_t numOutputReplicas = siteInfo.mNumOutputGen->GetValue(rngEngine);
                for(;numOutputReplicas>0; --numOutputReplicas)
                {
                    SpaceType size = siteInfo.mOutputSizeGen->GetValue(rngEngine);
                    SFile* outputFile = mSim->mRucio->CreateFile(size, now, SECONDS_PER_MONTH*6);
                    outputReplicas.emplace_back(outputSE->CreateReplica(outputFile, now));
                    cpuToOutputLink->mNumActiveTransfers += 1;
                    assert(outputReplicas.back());
                }
            }
            else if(!outputReplicas.empty())
            {
                //update uploads
                const SpaceType bytesUploaded = (cpuToOutputLink->mBandwidthBytesPerSecond / (double)(cpuToOutputLink->mNumActiveTransfers+1)) * tDelta;
                std::size_t idx = 0;
                while ( idx < outputReplicas.size() )
                {
                    SReplica* outputReplica = outputReplicas[idx];
                    
                    const SpaceType amount = outputReplica->Increase(bytesUploaded, now);
                    cpuToOutputLink->mUsedTraffic += amount;

                    if(outputReplica->IsComplete())
                    {
                        cpuToOutputLink->mNumActiveTransfers -= 1;
                        cpuToOutputLink->mNumDoneTransfers += 1;

                        traceInsertQueries->AddValue(GetNewId());
                        traceInsertQueries->AddValue(jobInfoIt->mJobId);
                        traceInsertQueries->AddValue(outputReplica->GetStorageElement()->GetId());
                        traceInsertQueries->AddValue(outputReplica->GetFile()->GetId());
                        traceInsertQueries->AddValue(outputReplica->GetId());
                        traceInsertQueries->AddValue(1);
                        traceInsertQueries->AddValue(jobInfoIt->mFinishedAt);
                        traceInsertQueries->AddValue(now);
                        traceInsertQueries->AddValue(outputReplica->GetFile()->GetSize());

                        //workaround to reduce memory load/output file not needed after trace is stored
                        mSim->mRucio->RemoveFile(outputReplica->GetFile(), now);

                        outputReplicas[idx] = outputReplicas.back();
                        outputReplicas.pop_back();

                        continue;
                    }
                    ++idx;
                }

                if(outputReplicas.empty())
                {
                    SReplica* inputSrcReplica = inputFile->GetReplicaByStorageElement(diskSE);
                    assert(inputSrcReplica);
                    if (inputSrcReplica->mNumStagedIn >= inputFile->mPopularity)
                        diskSE->RemoveReplica(inputSrcReplica, now);
                        //inputFile->RemoveReplica(now, inputSrcReplica); //inputSrcReplica could still be in use by disk -> CPU
                    
                    jobInfoIt = jobInfos.erase(jobInfoIt);
                    continue;
                }
            }
            jobInfoIt++;
        }

        assert(siteInfo.mNumCores >= jobInfos.size());

        std::size_t numJobsToCreate = std::min(siteInfo.mNumCores - jobInfos.size(), siteInfo.mCoreFillRate);
        for(const std::unique_ptr<SReplica>& replica : diskSE->GetReplicas()) // consider staging in the same replica multiple times
        {
            //create new jobs from diskSE replicas
            if(numJobsToCreate == 0)
                break;
            if(!replica->IsComplete())
                continue;
            if(replica->mNumStagedIn >= replica->GetFile()->mPopularity)
                continue;
            
            SJobInfo& newJobInfo = jobInfos.emplace_front();
            newJobInfo.mJobId = GetNewId();
            newJobInfo.mInputFile = replica->GetFile();
            
            replica->mNumStagedIn += 1;
            numJobsToCreate -= 1;
        }

        if(diskSE->GetUsedStorageQuotaRatio() <= 0.5)
        {
            //if storage on diskSE available create transfers from cloudSE to diskSE
            std::size_t numTransfers = (diskToCPULink->mMaxNumActiveTransfers - diskToCPULink->mNumActiveTransfers) / 2.0;
            for(const std::unique_ptr<SReplica>& replica : siteInfo.mCloudToDiskLink->GetSrcStorageElement()->GetReplicas())
            {
                if(numTransfers == 0)
                    break;
                if(!replica->IsComplete())
                    continue;

                SReplica* newReplica = diskSE->CreateReplica(replica->GetFile(), now);
                if (!newReplica)
                    continue;

                mTransferMgr->CreateTransfer(replica.get(), newReplica, now, true);
                numTransfers -= 1;
            }
        }
    }
    COutput::GetRef().QueueInserts(std::move(traceInsertQueries));
    mNextCallTick = now + mTickFreq;
}



CJobSlotTransferGen::CJobSlotTransferGen(IBaseSim* sim,
                                         std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                                         TickType tickFreq,
                                         TickType startTick )
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq)
{}

void CJobSlotTransferGen::OnUpdate(TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    const std::vector<std::unique_ptr<SFile>>& allFiles = mSim->mRucio->GetFiles();
    assert(allFiles.size() > 0);

    RNGEngineType& rngEngine = mSim->mRNGEngine;
    std::uniform_int_distribution<std::size_t> fileRndSelector(0, allFiles.size() - 1);


    for(auto& dstInfo : mDstInfo)
    {
        CStorageElement* dstStorageElement = dstInfo.first;
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
            SFile* fileToTransfer = allFiles[fileRndSelector(rngEngine)].get();

            for(std::uint32_t i = 0; i < 10 && fileToTransfer->GetReplicas().empty() && fileToTransfer->mExpiresAt < (now + 100); ++i)
                fileToTransfer = allFiles[fileRndSelector(rngEngine)].get();

            const std::vector<SReplica*>& replicas = fileToTransfer->GetReplicas();
            if(replicas.empty())
            {
                flexCreationLimit += 1;
                continue;
            }

            SReplica* newReplica = dstStorageElement->CreateReplica(fileToTransfer, now);
            if(newReplica != nullptr)
            {
                newReplica->mExpiresAt = now + SECONDS_PER_DAY;
                int minPrio = std::numeric_limits<int>::max();
                std::vector<SReplica*> bestSrcReplicas;
                for(SReplica* replica : replicas)
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

                SReplica* bestSrcReplica = bestSrcReplicas[0];
                if (minPrio > 0)
                {
                    double minWeight = std::numeric_limits<double>::max();
                    for (SReplica* replica : bestSrcReplicas)
                    {
                        double w = 0; //todo
                        if (w < minWeight)
                        {
                            minWeight = w;
                            bestSrcReplica = replica;
                        }
                    }
                }

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

    mNextCallTick = now + mTickFreq;
}



CCachedSrcTransferGen::CCachedSrcTransferGen(IBaseSim* sim,
                                             std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                                             std::size_t numPerDay,
                                             TickType defaultReplicaLifetime,
                                             TickType tickFreq,
                                             TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq),
      mNumPerDay(numPerDay),
      mDefaultReplicaLifetime(defaultReplicaLifetime)
{}

bool CCachedSrcTransferGen::ExistsFileAtStorageElement(const SFile* file, const CStorageElement* storageElement) const
{
    for(SReplica* r : file->GetReplicas())
    {
        if(r->GetStorageElement() == storageElement)
            return true;
    }
    return false;
}

void CCachedSrcTransferGen::ExpireReplica(CStorageElement* storageElement, const TickType now)
{
    const std::vector<std::unique_ptr<SReplica>>& replicas = storageElement->GetReplicas();
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
    (*oldestReplicaIt)->mExpiresAt = now;
    mSim->mRucio->RemoveExpiredReplicasFromFile((*oldestReplicaIt)->GetFile(), now);
}

void CCachedSrcTransferGen::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    RNGEngineType& rngEngine = mSim->mRNGEngine;

    const std::size_t numTotalTransfersPerUpdate = static_cast<std::size_t>((mNumPerDay * mTickFreq) / SECONDS_PER_DAY);
    for(auto it=mRatiosAndFilesPerAccessCount.rbegin(); it!=mRatiosAndFilesPerAccessCount.rend(); ++it)
    {
        const std::size_t numTransfersToCreate = 1 + static_cast<std::size_t>(numTotalTransfersPerUpdate * it->first);
        std::vector<SFile*>& fileVec = it->second;

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
                SFile* fileToTransfer = nullptr;
                for(std::size_t i=0; (i<numFileSelectRetries) && !fileToTransfer ; ++i)
                {
                    fileIdx = fileRndSelector(rngEngine);
                    fileToTransfer = fileVec[fileIdx];
                    if(!fileToTransfer)
                    {
                        std::swap(fileVec[fileIdx], fileVec.back());
                        fileVec.pop_back();
                        continue;
                    }
                    if(fileToTransfer->GetReplicas().empty() || ExistsFileAtStorageElement(fileToTransfer, dstStorageElement))
                        fileToTransfer = nullptr;
                }

                if(!fileToTransfer)
                    continue;

                //find best src replica
                SReplica* bestSrcReplica = nullptr;

                //check caches first
                if(!mCacheElements.empty())
                {
                    for(const SCacheElementInfo& cacheElement : mCacheElements)
                    {
                        for (SReplica* r : fileToTransfer->GetReplicas())
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
                    for(SReplica* srcReplica : fileToTransfer->GetReplicas())
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
                        if(cacheStorageElement->GetReplicas().size() >= mCacheElements[0].mCacheSize)
                            ExpireReplica(cacheStorageElement, now);

                        SReplica* newCacheReplica = cacheStorageElement->CreateReplica(fileToTransfer, now);
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

                SReplica* newReplica = dstStorageElement->CreateReplica(fileToTransfer, now);
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