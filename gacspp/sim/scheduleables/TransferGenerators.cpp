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

void CBaseOnDeletionInsert::PostCompleteReplica(SReplica * replica, TickType now)
{
    (void)replica;
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



CHCDCTransferGen::CHCDCTransferGen(IBaseSim* sim, std::shared_ptr<CTransferManager> transferMgr, TickType tickFreq, TickType startTick)
    : CSchedulable(startTick),
    mSim(sim),
    mTransferMgr(std::move(transferMgr)),
    mTickFreq(tickFreq),
    mLastUpdateTime(startTick)
{
    mDebugDurations.emplace_back("DeletionUpdate", DurationType::zero());
    mDebugDurations.emplace_back("WaitingUpdate", DurationType::zero());
    mDebugDurations.emplace_back("QueuedUpdate", DurationType::zero());
    mDebugDurations.emplace_back("ActiveUpdate", DurationType::zero());
    mDebugDurations.emplace_back("SubmitUpdate", DurationType::zero());
    mDebugDurations.emplace_back("Intern", DurationType::zero());
    mInputTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY InputTraces(id, jobId, siteId, storageElementId, fileId, replicaId, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 9, '?');
    mJobTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY JobTraces(id, siteId, createdAt, queuedAt, startedAt, finishedAt) FROM STDIN with(FORMAT csv);", 6, '?');
    mOutputTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY OutputTraces(id, jobId, siteId, storageElementId, fileId, replicaId, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 9, '?');
}

void CHCDCTransferGen::PostCompleteReplica(SReplica* replica, TickType now)
{
    (void)now;
    const CStorageElement* const replicaStorageElement = replica->GetStorageElement();
    std::uint32_t& popularity = replica->GetFile()->mPopularity;
    SFile* file = replica->GetFile();

    if (replicaStorageElement == mArchiveStorageElement)
    {
        //this case only works as long as replicas are only created once at archive storage
        std::vector<std::vector<SFile*>>& archiveFilesPerPopularity = mArchiveFilesPerPopularity;
        popularity = mReusageNumGen->GetValue(mSim->mRNGEngine);

        for (std::vector<SFile*>& files : archiveFilesPerPopularity)
        {
            if (files.empty() || (files.front()->mPopularity == popularity))
            {
                files.push_back(file);
                return;
            }
        }
        archiveFilesPerPopularity.emplace_back();
        archiveFilesPerPopularity.back().push_back(file);
    }
    else if (replicaStorageElement == mHotStorageElement)
    {
        JobInfoList& queuedJobs = mQueuedJobs;
        std::unordered_map<SReplica*, JobInfoList>::iterator transferringJobs = mTransferringJobs.find(replica);
        assert(transferringJobs != mTransferringJobs.end());

        for(std::unique_ptr<SJobInfo>& job : transferringJobs->second)
            job->mQueuedAt = now;

        queuedJobs.splice(queuedJobs.end(), std::move(transferringJobs->second));
        mTransferringJobs.erase(transferringJobs);
    }
}

void CHCDCTransferGen::PostCreateReplica(SReplica* replica, TickType now)
{
    (void)now;
    const CStorageElement* const replicaStorageElement = replica->GetStorageElement();
    std::uint32_t& popularity = replica->GetFile()->mPopularity;

    if (replicaStorageElement == mHotStorageElement)
    {
        auto res = mHotReplicasByPopularity.insert({ popularity, SIndexedReplicas() });
        bool wasAdded = res.first->second.AddReplica(replica);
        assert(wasAdded);
    }
}

void CHCDCTransferGen::PreRemoveReplica(SReplica* replica, TickType now)
{
    (void)now;
    const CStorageElement* const replicaStorageElement = replica->GetStorageElement();
    SFile* file = replica->GetFile();
    std::uint32_t popularity = file->mPopularity;

    if (replicaStorageElement == mHotStorageElement)
    {
        mHotReplicaDeletions.erase(replica);
        std::map<std::uint32_t, SIndexedReplicas>& hotReplicasByPopularity = mHotReplicasByPopularity;
            
        auto res = hotReplicasByPopularity.find(popularity);
        assert(res != hotReplicasByPopularity.end());

        bool wasRemovd = res->second.RemoveReplica(replica);
        assert(wasRemovd);

        if(res->second.IsEmpty())
            hotReplicasByPopularity.erase(res);
    }
}

void CHCDCTransferGen::OnUpdate(TickType now)
{
    CScopedTimeDiffAdd durationUpdate(mUpdateDurationSummed);

    if (mProductionStartTime > now)
    {
        // Create transfers archival -> hot; archival -> cold
        PrepareProductionCampaign(now);
    }
    else
    {
        // Create transfers archival -> hot; cold -> hot; hot -> cold;
        // Create and submit new jobs
        // Create stage-in; computation times; stage-out
        UpdateProductionCampaign(now);
    }

    mLastUpdateTime = now;

    mNextCallTick = now + mTickFreq;
}

void CHCDCTransferGen::Shutdown(const TickType now)
{
    (void)now;
    //remove this as archive storage element listener
    std::vector<IStorageElementActionListener*>& listeners1 = mArchiveStorageElement->mActionListener;
    auto listenerIt = listeners1.begin();
    auto listenerEnd = listeners1.end();
    for (; listenerIt != listenerEnd; ++listenerIt)
    {
        if ((*listenerIt) == this)
        {
            listeners1.erase(listenerIt);
            break;
        }
    }

    //remove this as hot storage element listener
    std::vector<IStorageElementActionListener*>& listeners2 = mHotStorageElement->mActionListener;
    listenerIt = listeners2.begin();
    listenerEnd = listeners2.end();
    for (; listenerIt != listenerEnd; ++listenerIt)
    {
        if ((*listenerIt) == this) 
        {
            listeners2.erase(listenerIt);
            break;
        }
    }
}

std::discrete_distribution<std::size_t> CHCDCTransferGen::GetPopularityIdxRNG()
{
    std::vector<std::uint32_t> weights;
    weights.reserve(mArchiveFilesPerPopularity.size());
    for (const std::vector<SFile*>& files : mArchiveFilesPerPopularity)
        weights.push_back(files.front()->mPopularity);
    return std::discrete_distribution<std::size_t>(weights.begin(), weights.end());

}

void CHCDCTransferGen::QueueHotReplicasDeletion(SReplica* replica, TickType expireAt)
{
    mHotReplicasDeletionQueue[expireAt].emplace_back(replica);
    mHotReplicaDeletions.insert(replica);
}

void CHCDCTransferGen::PrepareProductionCampaign(TickType now)
{
    std::discrete_distribution<std::size_t> popularityIdxRNG = GetPopularityIdxRNG();

    assert(mArchiveToHotLink->mMaxNumActiveTransfers >= mArchiveToHotLink->mNumActiveTransfers);
    std::size_t hotReplicasCreationLimit = mArchiveToHotLink->mMaxNumActiveTransfers - mArchiveToHotLink->mNumActiveTransfers;

    assert(mArchiveToColdLink->mMaxNumActiveTransfers >= mArchiveToColdLink->mNumActiveTransfers);
    std::size_t coldReplicaCreationLimit = mArchiveToColdLink->mMaxNumActiveTransfers - mArchiveToColdLink->mNumActiveTransfers;

    std::uint32_t maxRetries = 100;

    while ((hotReplicasCreationLimit > 0) && (maxRetries > 0) && !mArchiveFilesPerPopularity.empty())
    {
        //chose random popularity and get all archive file of it
        std::vector<SFile*>& files = mArchiveFilesPerPopularity[popularityIdxRNG(mSim->mRNGEngine)];

        //create random file selector
        std::uniform_int_distribution<std::size_t> fileIdxRNG(0, files.size() - 1);

        assert(!files.empty());

        //get a random file
        SFile* srcFile = files[fileIdxRNG(mSim->mRNGEngine)];

        SReplica* newReplica;
        if (mHotStorageElement->CanStoreVolume(srcFile->GetSize()))
        {
            newReplica = mHotStorageElement->CreateReplica(srcFile, now);
            if (!newReplica)
            {
                --maxRetries;
                continue;
            }

            maxRetries = 100;
            hotReplicasCreationLimit -= 1;
        }
        else if (coldReplicaCreationLimit > 0)
        {
            //not enough storage at hot storage so try cold storage
            newReplica = mColdStorageElement->CreateReplica(srcFile, now);
            if (!newReplica)
                break;

            coldReplicaCreationLimit -= 1;
        }
        else
            break;

        SReplica* srcReplica = srcFile->GetReplicaByStorageElement(mArchiveStorageElement);
        assert(srcReplica && srcReplica->IsComplete());

        mTransferMgr->CreateTransfer(srcReplica, newReplica, now);
    }
}

void CHCDCTransferGen::UpdateProductionCampaign(TickType now)
{
    //delete unused expired replicas
    {
        CScopedTimeDiffAdd durationUpdate(mDebugDurations[0].second);
        UpdatePendingDeletions(now);
    }
    
    {
        CScopedTimeDiffAdd durationUpdate(mDebugDurations[1].second);
        UpdateWaitingJobs(now);
    }

    {
        CScopedTimeDiffAdd durationUpdate(mDebugDurations[2].second);
        UpdateQueuedJobs(now);
    }

    {
        CScopedTimeDiffAdd durationUpdate(mDebugDurations[3].second);
        UpdateActiveJobs(now);
    }

    {
        CScopedTimeDiffAdd durationUpdate(mDebugDurations[4].second);
        SubmitNewJobs(now);
    }
}

SpaceType CHCDCTransferGen::DeleteQueuedHotReplicas(TickType now)
{
    SpaceType requiredSpace = 0;
    std::map<TickType, std::vector<SReplica*>>::iterator queueIt = mHotReplicasDeletionQueue.begin();
    const std::map<TickType, std::vector<SReplica*>>::iterator expiredEntries = mHotReplicasDeletionQueue.lower_bound(now + 1);

    // only use cold storage if no limit is set or limit is greater than 1 MiB
    const bool isColdStorageEnabled = (mColdStorageElement->GetLimit() == 0) || (mColdStorageElement->GetLimit() > ONE_MiB);

    while (queueIt != expiredEntries)
    {
        std::vector<SReplica*>& hotReplicasToDelete = queueIt->second;
        assert(!hotReplicasToDelete.empty());

        for (std::size_t hotReplicaIdx = 0; hotReplicaIdx < hotReplicasToDelete.size();)
        {
            SReplica* hotReplica = hotReplicasToDelete[hotReplicaIdx];
            assert(hotReplica->mUsageCounter == 0);

            SFile* file = hotReplica->GetFile();
            SReplica* coldStorageReplica = file->GetReplicaByStorageElement(mColdStorageElement);
            if (!coldStorageReplica && isColdStorageEnabled)
            {
                SReplica* dstReplica = mColdStorageElement->CreateReplica(file, now);

                if (!dstReplica)
                {
                    //if cold storage is limited and replica could not be created free some cold storage
                    requiredSpace += file->GetSize();
                    ++hotReplicaIdx;
                    continue;
                }
                else
                {
                    //transfer to cold storage prior to deleltion
                    mColdReplicasByPopularity.emplace(std::make_pair(file->mPopularity, std::forward_list<SReplica*>())).first->second.emplace_front(dstReplica);
                    mTransferMgr->CreateTransfer(hotReplica, dstReplica, now, true);
                }
            }
            else
            {
                //cold storage replica exists already or cold storage is not available
                mHotStorageElement->RemoveReplica(hotReplica, now);
            }

            hotReplicasToDelete[hotReplicaIdx] = hotReplicasToDelete.back();
            hotReplicasToDelete.pop_back();
        }

        if (hotReplicasToDelete.empty())
            queueIt = mHotReplicasDeletionQueue.erase(queueIt);
        else
            ++queueIt;
    }

    return requiredSpace;
}

void CHCDCTransferGen::UpdatePendingDeletions(TickType now)
{
    SpaceType requiredSpace = DeleteQueuedHotReplicas(now) * 1;
    if (requiredSpace > 0)
    {
        auto popularityIt = mColdReplicasByPopularity.begin();
        while (popularityIt != mColdReplicasByPopularity.end() && requiredSpace > 0)
        {
            std::forward_list<SReplica*>& coldReplicas = popularityIt->second;
            auto prevReplicaIt = coldReplicas.before_begin();
            auto curReplicaIt = coldReplicas.begin();
            while (curReplicaIt != coldReplicas.end() && requiredSpace > 0)
            {
                SReplica* coldReplica = *curReplicaIt;
                if (coldReplica->mUsageCounter != 0)
                {
                    ++prevReplicaIt;
                    ++curReplicaIt;
                    continue;
                }

                requiredSpace = (coldReplica->GetCurSize() < requiredSpace) ? (requiredSpace - coldReplica->GetCurSize()) : 0;
                mColdStorageElement->RemoveReplica(coldReplica, now);

                curReplicaIt = coldReplicas.erase_after(prevReplicaIt);
            }
            ++popularityIt;
        }

        DeleteQueuedHotReplicas(now);
    }
}

void CHCDCTransferGen::UpdateWaitingJobs(TickType now)
{
    while (!mWaitingJobs.empty() && mHotStorageElement->CanStoreVolume(mWaitingJobs.front()->mInputFile->GetSize()))
    {
        SFile* file = mWaitingJobs.front()->mInputFile;

        // storage is free so create a transfer and notify all waiting jobs
        SReplica* newReplica = mHotStorageElement->CreateReplica(file, now);
        assert(newReplica);
        
        SReplica* srcReplica = file->GetReplicaByStorageElement(mColdStorageElement);
        if (!srcReplica)
            srcReplica = file->GetReplicaByStorageElement(mArchiveStorageElement);
        assert(srcReplica);

        mTransferMgr->CreateTransfer(srcReplica, newReplica, now);

        // move all jobs that have been waiting for this replica into transferring
        JobInfoList& transferringList = mTransferringJobs[newReplica];
        auto findResult = mWaitingForSameFile.find(file);
        assert(findResult != mWaitingForSameFile.end());
        newReplica->mUsageCounter += findResult->second.size();
        for (JobInfoList::iterator& it : findResult->second)
        {
            (*it)->mInputReplica = newReplica;
            transferringList.splice(transferringList.end(), mWaitingJobs, it);
        }
        mWaitingForSameFile.erase(findResult);
    }
}

void CHCDCTransferGen::UpdateQueuedJobs(TickType now)
{
    (void)now;

    assert(mNumCores >= mNumJobs);
    std::size_t numJobsToActivate = mNumCores - mNumJobs;

    if(numJobsToActivate < mQueuedJobs.size())
    {
        JobInfoList::iterator queuedJobIt = mQueuedJobs.begin();
        while((queuedJobIt != mQueuedJobs.end()) && (numJobsToActivate > 0))
        {
            mNewJobs.splice(mNewJobs.end(), mQueuedJobs, queuedJobIt++);
            numJobsToActivate -= 1;
        }
    }
    else if(numJobsToActivate > 0)
        mNewJobs.splice(mNewJobs.end(), std::move(mQueuedJobs));
}

void CHCDCTransferGen::UpdateActiveJobs(TickType now)
{
    const TickType tDelta = now - mLastUpdateTime;

    std::unique_ptr<IInsertValuesContainer> inputTraceInsertQueries = mInputTraceInsertQuery->CreateValuesContainer(9 * 30);
    std::unique_ptr<IInsertValuesContainer> jobTraceInsertQueries = mJobTraceInsertQuery->CreateValuesContainer(6 * 30);
    std::unique_ptr<IInsertValuesContainer> outputTraceInsertQueries = mOutputTraceInsertQuery->CreateValuesContainer(9 * 30);


    //should firstly update all mNumActiveTransfers and then calculate bytesDownloaded/uploaded
    SpaceType bytesDownloaded = (mHotToCPULink->mBandwidthBytesPerSecond * tDelta);
    if (!mHotToCPULink->mIsThroughput)
        bytesDownloaded /= static_cast<double>(mHotToCPULink->mNumActiveTransfers) + 1;

    // update downloading jobs before adding the new jobs
    JobInfoList::iterator jobIt = mDownloadingJobs.begin();
    while(jobIt != mDownloadingJobs.end())
    {
        std::unique_ptr<SJobInfo>& job = *jobIt;
        SFile* inputFile = job->mInputFile;
        const SpaceType newSize = job->mCurInputFileSize + bytesDownloaded;
        if (newSize >= inputFile->GetSize())
        {
            //download completed
            mHotToCPULink->mUsedTraffic += inputFile->GetSize() - job->mCurInputFileSize;
            mHotToCPULink->mNumActiveTransfers -= 1;
            mHotToCPULink->mNumDoneTransfers += 1;

            job->mCurInputFileSize = inputFile->GetSize();

            inputTraceInsertQueries->AddValue(GetNewId());
            inputTraceInsertQueries->AddValue(job->mJobId);
            inputTraceInsertQueries->AddValue(mHotStorageElement->GetSite()->GetId());
            inputTraceInsertQueries->AddValue(mHotStorageElement->GetId());
            inputTraceInsertQueries->AddValue(inputFile->GetId());
            inputTraceInsertQueries->AddValue(job->mInputReplica->GetId());
            inputTraceInsertQueries->AddValue(job->mLastTime);
            inputTraceInsertQueries->AddValue(now);
            inputTraceInsertQueries->AddValue(inputFile->GetSize());

            TickType finishTime = now + (static_cast<TickType>(mJobDurationGen->GetValue(mSim->mRNGEngine)) * 60);

            //insert job trace directly so that jobId of input trace points to a valid jobId
            jobTraceInsertQueries->AddValue(job->mJobId);
            jobTraceInsertQueries->AddValue(mHotStorageElement->GetSite()->GetId());
            jobTraceInsertQueries->AddValue(job->mCreatedAt);
            jobTraceInsertQueries->AddValue(job->mQueuedAt);
            jobTraceInsertQueries->AddValue(now);
            jobTraceInsertQueries->AddValue(finishTime);

            job->mLastTime = finishTime;

            //move job from jobInfos list to mRunningJobs while it is 'idling'
            JobInfoList& runningJobsAtFinishTime = mRunningJobs[finishTime];
            runningJobsAtFinishTime.splice(runningJobsAtFinishTime.end(), mDownloadingJobs, jobIt++);
            continue;
        }
        else
        {
            mHotToCPULink->mUsedTraffic += bytesDownloaded;
            job->mCurInputFileSize = newSize;
        }
        ++jobIt;
    }


    // add new jobs
    mNumJobs += mNewJobs.size();
    mHotToCPULink->mNumActiveTransfers += mNewJobs.size();
    for(std::unique_ptr<SJobInfo>& job : mNewJobs)
    {
        //download just started
        job->mLastTime = now;
        mHotToCPULink->GetSrcStorageElement()->OnOperation(CStorageElement::GET);
    }
    mDownloadingJobs.splice(mDownloadingJobs.end(), std::move(mNewJobs));


    // put finished jobs to uploading state
    {
        std::map<TickType, JobInfoList>::iterator runningJobsIt = mRunningJobs.begin();
        while((runningJobsIt != mRunningJobs.end()) && (runningJobsIt->first <= now))
        {
            // create unlock input and create output
            for(std::unique_ptr<SJobInfo>& job : runningJobsIt->second)
            {
                std::vector<SReplica*>& outputReplicas = job->mOutputReplicas;
                job->mLastTime = now;

                // unlock input replica at hot storage
                job->mInputReplica->mUsageCounter -= 1;
                if(job->mInputReplica->mUsageCounter == 0 && (mHotStorageElement->GetLimit() > 0))
                    QueueHotReplicasDeletion(job->mInputReplica, now + 90 + static_cast<TickType>(job->mInputFile->GetSize()/MiB_TO_BYTES(500.0)));

                //todo: consider mCPUToOutputLink->mMaxNumActiveTransfers
                std::size_t numOutputReplicas = mNumOutputGen->GetValue(mSim->mRNGEngine);
                mCPUToOutputLink->mNumActiveTransfers += numOutputReplicas;
                for (; numOutputReplicas > 0; --numOutputReplicas)
                {
                    const SpaceType size = static_cast<SpaceType>(GiB_TO_BYTES(mOutputSizeGen->GetValue(mSim->mRNGEngine)));
                    SFile* outputFile = mSim->mRucio->CreateFile(size, now, SECONDS_PER_MONTH * 12);
                    outputReplicas.emplace_back(mCPUToOutputLink->GetDstStorageElement()->CreateReplica(outputFile, now));
                    outputReplicas.back()->mUsageCounter += 1;
                    mCPUToOutputLink->GetSrcStorageElement()->OnOperation(CStorageElement::GET);
                    assert(outputReplicas.back());
                }
            }
            mUploadingJobs.splice(mUploadingJobs.end(), std::move(runningJobsIt->second));
            ++runningJobsIt;
        }
        mRunningJobs.erase(mRunningJobs.begin(), runningJobsIt);
    }


    // update uploads
    SpaceType bytesUploaded = (mCPUToOutputLink->mBandwidthBytesPerSecond * tDelta);
    if(!mCPUToOutputLink->mIsThroughput)
        bytesUploaded /= static_cast<double>(mCPUToOutputLink->mNumActiveTransfers) + 1;
    
    jobIt = mUploadingJobs.begin();
    while(jobIt != mUploadingJobs.end())
    {
        std::unique_ptr<SJobInfo>& job = *jobIt;
        std::vector<SReplica*>& outputReplicas = job->mOutputReplicas;
        std::size_t idx = 0;
        while (idx < outputReplicas.size())
        {
            SReplica* outputReplica = outputReplicas[idx];

            const SpaceType amount = outputReplica->Increase(bytesUploaded, now);
            mCPUToOutputLink->mUsedTraffic += amount;

            if (outputReplica->IsComplete())
            {
                outputReplica->mUsageCounter -= 1;
                mCPUToOutputLink->mNumActiveTransfers -= 1;
                mCPUToOutputLink->mNumDoneTransfers += 1;

                outputTraceInsertQueries->AddValue(GetNewId());
                outputTraceInsertQueries->AddValue(job->mJobId);
                outputTraceInsertQueries->AddValue(outputReplica->GetStorageElement()->GetSite()->GetId());
                outputTraceInsertQueries->AddValue(outputReplica->GetStorageElement()->GetId());
                outputTraceInsertQueries->AddValue(outputReplica->GetFile()->GetId());
                outputTraceInsertQueries->AddValue(outputReplica->GetId());
                outputTraceInsertQueries->AddValue(job->mLastTime);
                outputTraceInsertQueries->AddValue(now);
                outputTraceInsertQueries->AddValue(outputReplica->GetFile()->GetSize());

                //workaround to reduce memory load/output file not needed after trace is stored
                mSim->mRucio->RemoveFile(outputReplica->GetFile(), now);

                outputReplicas[idx] = outputReplicas.back();
                outputReplicas.pop_back();

                continue;
            }
            ++idx;
        }

        if (outputReplicas.empty())
        {
            mNumJobs -= 1;
            jobIt = mUploadingJobs.erase(jobIt);
        }
    }

    COutput::GetRef().QueueInserts(std::move(inputTraceInsertQueries));
    COutput::GetRef().QueueInserts(std::move(jobTraceInsertQueries));
    //COutput::GetRef().QueueInserts(std::move(outputTraceInsertQueries)); //dont write output traces to DB for now
}

void CHCDCTransferGen::SubmitNewJobs(TickType now)
{
    if(mArchiveFilesPerPopularity.empty())
        return;

    std::uint64_t numToCreate;
    {
        const double val = mNumJobSubmissionGen->GetValue(mSim->mRNGEngine) + mNumJobSubmissionAccu;
        numToCreate = val;
        mNumJobSubmissionAccu = val - numToCreate;
    }
    
    std::discrete_distribution<std::size_t> popularityIdxRNG = GetPopularityIdxRNG();
    for (; numToCreate > 0; --numToCreate)
    {
        std::vector<SFile*>& files = mArchiveFilesPerPopularity[popularityIdxRNG(mSim->mRNGEngine)];
        const std::size_t numFiles = files.size();
        const std::size_t randomFileIdxOrigin = std::uniform_int_distribution<std::size_t>(0, numFiles - 1)(mSim->mRNGEngine);
        std::size_t randomFileIdxCur = randomFileIdxOrigin;

        SFile* inputFile;
        SReplica* inputReplica;
        do
        {
            inputFile = files[randomFileIdxCur];
            inputReplica = inputFile->GetReplicaByStorageElement(mHotStorageElement);
            randomFileIdxCur = (randomFileIdxCur + 1) % numFiles;
        } while ((randomFileIdxCur != randomFileIdxOrigin) && (mHotReplicaDeletions.count(inputReplica) > 0));
        
        assert((randomFileIdxCur != randomFileIdxOrigin) || (mHotReplicaDeletions.count(inputReplica) == 0));

        std::unique_ptr<SJobInfo> newJob = std::make_unique<SJobInfo>();
        newJob->mJobId = GetNewId();
        newJob->mCreatedAt = now;
        newJob->mInputFile = inputFile;
        newJob->mInputReplica = inputReplica;
        if (inputReplica)
        {
            //input replica exists already 
            inputReplica->mUsageCounter += 1;
            if(inputReplica->IsComplete())
            {
                //transfer already done -> queue
                newJob->mQueuedAt = now;
                mQueuedJobs.emplace_back(std::move(newJob));
            }
            else //transfer in progress -> transferring
                mTransferringJobs[inputReplica].emplace_back(std::move(newJob));
        }
        else
        {
            //input replica not there -> add to waiting list
            auto wjobit = mWaitingJobs.emplace(mWaitingJobs.end(), std::move(newJob));
            mWaitingForSameFile[inputFile].push_back(wjobit);
        }
    }
}



CCloudBufferTransferGen::CCloudBufferTransferGen(IBaseSim* sim,
                                                 std::shared_ptr<CTransferManager> transferMgr,
                                                 TickType tickFreq,
                                                 TickType startTick )
    : CSchedulable(startTick),
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

void CCloudBufferTransferGen::PostCompleteReplica(SReplica * replica, TickType now)
{
    (void)now;
    for (std::unique_ptr<STransferGenInfo>& info : mTransferGenInfo)
    {
        if (replica->GetStorageElement() == info->mPrimaryLink->GetSrcStorageElement())
        {
            const std::uint32_t numReusages = info->mReusageNumGen->GetValue(mSim->mRNGEngine);
            replica->GetFile()->mPopularity = numReusages;
            std::forward_list<SReplica*>& replicas = info->mReplicas;
            auto prev = replicas.before_begin();
            auto cur = replicas.begin();
            while (cur != replicas.end())
            {
                if ((*cur)->GetFile()->mPopularity >= numReusages)
                    break;

                prev = cur;
                cur++;
            }
            replicas.insert_after(prev, replica);
            return;
        }
    }
}

void CCloudBufferTransferGen::PostCreateReplica(SReplica* replica, TickType now)
{
    (void)replica;
    (void)now;
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
    CScopedTimeDiffAdd durationUpdate(mUpdateDurationSummed);

    assert(!mTransferGenInfo.empty());

    for (std::unique_ptr<STransferGenInfo>& info : mTransferGenInfo)
    {
        CNetworkLink* networkLink = info->mPrimaryLink;
        CNetworkLink* secondNetworkLink = info->mSecondaryLink;
        std::forward_list<SReplica*>& replicas = info->mReplicas;

        assert(networkLink->mMaxNumActiveTransfers >= networkLink->mNumActiveTransfers);
        std::size_t numToCreate = networkLink->mMaxNumActiveTransfers - networkLink->mNumActiveTransfers;

        assert(secondNetworkLink->mMaxNumActiveTransfers >= secondNetworkLink->mNumActiveTransfers);
        std::size_t numToCreateSecondary = secondNetworkLink->mMaxNumActiveTransfers - secondNetworkLink->mNumActiveTransfers;

        SpaceType volumeSum = 0;

        auto prev = replicas.before_begin();
        auto cur = replicas.begin();
        while ((cur != replicas.end()) && (numToCreate > 0))
        {
            SReplica* srcReplica = (*cur);

            assert(srcReplica->IsComplete());

            SReplica* newReplica = nullptr;
            SFile* file = srcReplica->GetFile();
            volumeSum += file->GetSize();

            if (networkLink->GetDstStorageElement()->CanStoreVolume(volumeSum))
            {
                newReplica = networkLink->GetDstStorageElement()->CreateReplica(file, now);
                assert(newReplica);

                numToCreate -= 1;
            }
            else if (numToCreateSecondary > 0)
            {
                newReplica = secondNetworkLink->GetDstStorageElement()->CreateReplica(file, now);
                if (!newReplica)
                    break;

                numToCreateSecondary -= 1;
            }
            else
                break;

            mTransferMgr->CreateTransfer(srcReplica, newReplica, now, mDeleteSrcReplica);
            cur = replicas.erase_after(prev);
        }
    }

    mNextCallTick = now + mTickFreq;
}



CJobSlotTransferGen::CJobSlotTransferGen(IBaseSim* sim,
                                         std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                                         TickType tickFreq,
                                         TickType startTick )
    : CSchedulable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq)
{}

void CJobSlotTransferGen::OnUpdate(TickType now)
{
    CScopedTimeDiffAdd durationUpdate(mUpdateDurationSummed);

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
    : CSchedulable(startTick),
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
    CScopedTimeDiffAdd durationUpdate(mUpdateDurationSummed);

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



CFixedTransferGen::CFixedTransferGen(IBaseSim* sim,
                    std::shared_ptr<CTransferManager> transferMgr,
                    TickType tickFreq,
                    TickType startTick)
    : CSchedulable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq)
{}

void CFixedTransferGen::PostCompleteReplica(SReplica* replica, TickType now)
{
    if(now>0)
        mCompleteReplicas.push_back(replica);
}
void CFixedTransferGen::PostCreateReplica(SReplica* replica, TickType now)
{
    (void)replica;
    (void)now;
}
void CFixedTransferGen::PreRemoveReplica(SReplica* replica, TickType now)
{
    (void)replica;
    (void)now;
}

void CFixedTransferGen::OnUpdate(TickType now)
{
    CScopedTimeDiffAdd durationUpdate(mUpdateDurationSummed);

    while(!mCompleteReplicas.empty())
    {
        mCompleteReplicas.back()->GetStorageElement()->RemoveReplica(mCompleteReplicas.back(), now);
        mCompleteReplicas.pop_back();
    }

    auto& rngEngine = mSim->mRNGEngine;
    for(std::pair<CStorageElement*, std::vector<STransferGenInfo>>& cfg : mConfig)
    {
        const CStorageElement* srcStorageElement = cfg.first;
        const std::vector<std::unique_ptr<SReplica>>& srcReplicas = srcStorageElement->GetReplicas();
        const std::size_t numSrcReplicas = srcReplicas.size();
        assert(numSrcReplicas > 0);
        for(STransferGenInfo& info : cfg.second)
        {
            CStorageElement* dstStorageElement = info.mDstStorageElement;
            
            const double val = info.mNumTransferGen->GetValue(rngEngine) + info.mDecimalAccu;
            std::uint64_t numToCreate = val;
            info.mDecimalAccu = val - numToCreate;

            while(numToCreate > 0)
            {
                std::uniform_int_distribution<std::size_t> replicaIdxRNG(0, numSrcReplicas - 1);
                const std::size_t rngIdxOffset = replicaIdxRNG(rngEngine);
                std::size_t i = 0;
                for(; i<numSrcReplicas; ++i)
                {
                    SReplica* srcReplica = srcReplicas[(rngIdxOffset + i) % numSrcReplicas].get();
                    SFile* srcFile = srcReplica->GetFile();
                    if(srcReplica->IsComplete() && !srcFile->GetReplicaByStorageElement(dstStorageElement))
                    {
                        SReplica* dstReplica = dstStorageElement->CreateReplica(srcFile, now);
                        assert(dstReplica);
                        mTransferMgr->CreateTransfer(srcReplica, dstReplica, now);
                        --numToCreate;
                        break;
                    }
                }

                // no src replicas available
                if(i == numSrcReplicas)
                {
                    // approach 1
                    assert(false);

                    // approach 2
                    break; 

                    // approach 3
                    SFile* tmplFile = srcReplicas[rngIdxOffset]->GetFile();
                    SFile* srcFile = mSim->mRucio->CreateFile(tmplFile->GetSize(), now, tmplFile->mExpiresAt);
                    SReplica* srcReplica = cfg.first->CreateReplica(srcFile, now);
                    srcReplica->Increase(srcFile->GetSize(), now);
                    SReplica* dstReplica = dstStorageElement->CreateReplica(srcFile, now);
                    mTransferMgr->CreateTransfer(srcReplica, dstReplica, now);
                    --numToCreate;
                }
            }
        }
    }

    mNextCallTick = now + mTickFreq;
}

void CFixedTransferGen::Shutdown(const TickType now)
{
    (void)now;
    for(const std::pair<CStorageElement*, std::vector<STransferGenInfo>>& cfg : mConfig)
    {
        for(const STransferGenInfo& info : cfg.second)
        {
            std::vector<IStorageElementActionListener*>& listeners1 = info.mDstStorageElement->mActionListener;
            auto listenerIt = listeners1.begin();
            auto listenerEnd = listeners1.end();
            for (; listenerIt != listenerEnd; ++listenerIt)
            {
                if ((*listenerIt) == this)
                {
                    listeners1.erase(listenerIt);
                    break;
                }
            }
        }
    }
}
