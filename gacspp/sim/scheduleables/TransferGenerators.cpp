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



CHotColdStorageTransferGen::CHotColdStorageTransferGen(IBaseSim* sim, std::shared_ptr<CTransferManager> transferMgr, TickType tickFreq, TickType startTick)
    : CScheduleable(startTick),
    mSim(sim),
    mTransferMgr(std::move(transferMgr)),
    mTickFreq(tickFreq),
    mLastUpdateTime(startTick)
{
    mInputTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY InputTraces(id, jobId, siteId, storageElementId, fileId, replicaId, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 9, '?');
    mJobTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY JobTraces(id, siteId, createdAt, queuedAt, startedAt, finishedAt) FROM STDIN with(FORMAT csv);", 6, '?');
    mOutputTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY OutputTraces(id, jobId, siteId, storageElementId, fileId, replicaId, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 9, '?');
}

CHotColdStorageTransferGen::~CHotColdStorageTransferGen()
{
    for (SSiteInfo& siteInfo : mSiteInfos)
    {
        //remove this as archive storage element listener
        std::vector<IStorageElementActionListener*> listeners = siteInfo.mArchiveToHotLink->GetSrcStorageElement()->mActionListener;
        auto listenerIt = listeners.begin();
        auto listenerEnd = listeners.end();
        for (; listenerIt != listenerEnd; ++listenerIt)
        {
            if ((*listenerIt) == this)
            {
                listeners.erase(listenerIt);
                break;
            }
        }

        //remove this as hot storage element listener
        listeners = siteInfo.mArchiveToHotLink->GetDstStorageElement()->mActionListener;
        listenerIt = listeners.begin();
        listenerEnd = listeners.end();
        for (; listenerIt != listenerEnd; ++listenerIt)
        {
            if ((*listenerIt) == this)
            {
                listeners.erase(listenerIt);
                break;
            }
        }
    }
}

void CHotColdStorageTransferGen::PostCompleteReplica(SReplica* replica, TickType now)
{
    (void)now;
    CStorageElement* replicaStorageElement = replica->GetStorageElement();
    std::uint32_t& popularity = replica->GetFile()->mPopularity;
    SFile* file = replica->GetFile();

    for(SSiteInfo& siteInfo : mSiteInfos)
    {
        if (replicaStorageElement == siteInfo.mArchiveToHotLink->GetSrcStorageElement())
        {
            //this case only works as long as replicas are only created once at archive storage
            auto& archiveFilesPerPopularity = siteInfo.mArchiveFilesPerPopularity;
            popularity = siteInfo.mReusageNumGen->GetValue(mSim->mRNGEngine);

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
            break;
        }
        else if(replicaStorageElement == siteInfo.mArchiveToHotLink->GetDstStorageElement())
        {
            std::map<std::uint32_t, SIndexedReplicas>& hotReplicas = siteInfo.mHotReplicasByPopularity;
            std::map<std::uint32_t, SIndexedReplicas>& unusedHotReplicas = siteInfo.mUnusedHotReplicasByPopularity;
            auto res = hotReplicas.insert({popularity, SIndexedReplicas()});
            bool wasAdded = res.first->second.AddReplica(replica);
            assert(wasAdded);
            
            if(replica->mUsageCounter <= 1)
            {
                res = unusedHotReplicas.insert({popularity, SIndexedReplicas()});
                wasAdded = res.first->second.AddReplica(replica);
                assert(wasAdded);
            }
            break;
        }
    }
}

void CHotColdStorageTransferGen::PostCreateReplica(SReplica* replica, TickType now)
{
    (void)replica;
    (void)now;
}

void CHotColdStorageTransferGen::PreRemoveReplica(SReplica* replica, TickType now)
{
    (void)now;
    CStorageElement* replicaStorageElement = replica->GetStorageElement();
    SFile* file = replica->GetFile();
    std::uint32_t popularity = file->mPopularity;

    for (SSiteInfo& siteInfo : mSiteInfos)
    {
        siteInfo.mHotReplicaDeletions.erase(replica);
        if (replicaStorageElement == siteInfo.mArchiveToHotLink->GetDstStorageElement())
        {
            std::map<std::uint32_t, SIndexedReplicas>& hotReplicasByPopularity = siteInfo.mHotReplicasByPopularity;
            
            auto res = hotReplicasByPopularity.find(popularity);
            assert(res != hotReplicasByPopularity.end());

            bool wasRemovd = res->second.RemoveReplica(replica);
            assert(wasRemovd);

            if(res->second.IsEmpty())
                hotReplicasByPopularity.erase(res);
            
            break;
        }
        else
            assert(replicaStorageElement != siteInfo.mArchiveToHotLink->GetSrcStorageElement()); //archive replicas must not be deleted
    }
}

void CHotColdStorageTransferGen::OnUpdate(TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);
    for (SSiteInfo& siteInfo : mSiteInfos)
    {
        if (siteInfo.mProductionStartTime > now)
        {
            // Create transfers archival -> hot; archival -> cold
            PrepareProductionCampaign(siteInfo, now);
        }
        else
        {
            // Create transfers archival -> hot; cold -> hot; hot -> cold;
            // Create and submit new jobs
            // Create stage-in; computation times; stage-out
            UpdateProductionCampaign(siteInfo, now);
        }
    }

    mNextCallTick = now + mTickFreq;
}

std::discrete_distribution<std::size_t> CHotColdStorageTransferGen::GetPopularityIdxRNG(const SSiteInfo& siteInfo)
{
    const auto& archiveFilesPerPopularity = siteInfo.mArchiveFilesPerPopularity;
    std::vector<std::uint32_t> weights;
    weights.reserve(archiveFilesPerPopularity.size());
    for (const std::vector<SFile*>& files : archiveFilesPerPopularity)
        weights.push_back(files.front()->mPopularity);
    return std::discrete_distribution<std::size_t>(weights.begin(), weights.end());

}

void CHotColdStorageTransferGen::CreateJobInputTransfer(CStorageElement* archiveStorageElement, CStorageElement* coldStorageElement, CStorageElement* hotStorageElement, SJobInfo* job, TickType now)
{
    SFile* file = job->mInputFile;
    SReplica* newReplica = hotStorageElement->CreateReplica(file, now);
    if (newReplica)
    {
        SReplica* srcReplica = file->GetReplicaByStorageElement(coldStorageElement);
        if (!srcReplica)
            srcReplica = file->GetReplicaByStorageElement(archiveStorageElement);

        assert(srcReplica);

        mTransferMgr->CreateTransfer(srcReplica, newReplica, now);
    }
    else
        newReplica = file->GetReplicaByStorageElement(hotStorageElement);

    assert(newReplica);

    newReplica->mUsageCounter += 1;
    job->mInputReplica = newReplica;
    job->mQueuedAt = now;
}

void CHotColdStorageTransferGen::PrepareProductionCampaign(SSiteInfo& siteInfo, TickType now)
{
    std::discrete_distribution<std::size_t> popularityIdxRNG = GetPopularityIdxRNG(siteInfo);
    auto& archiveFilesPerPopularity = siteInfo.mArchiveFilesPerPopularity;

    CNetworkLink* archiveToCold = siteInfo.mArchiveToColdLink;
    CNetworkLink* archiveToHot = siteInfo.mArchiveToHotLink;

    CStorageElement* archiveStorageElement = archiveToCold->GetSrcStorageElement();
    CStorageElement* coldStorageElement = archiveToCold->GetDstStorageElement();
    CStorageElement* hotStorageElement = archiveToHot->GetDstStorageElement();

    assert(archiveToHot->mMaxNumActiveTransfers >= archiveToHot->mNumActiveTransfers);
    std::size_t hotReplicasCreationLimit = archiveToHot->mMaxNumActiveTransfers - archiveToHot->mNumActiveTransfers;

    assert(archiveToCold->mMaxNumActiveTransfers >= archiveToCold->mNumActiveTransfers);
    std::size_t coldReplicaCreationLimit = archiveToCold->mMaxNumActiveTransfers - archiveToCold->mNumActiveTransfers;

    std::uint32_t maxRetries = 100;
    bool hotStorageFull = false;

    while ((hotReplicasCreationLimit > 0) && (maxRetries > 0) && !archiveFilesPerPopularity.empty())
    {
        //chose random popularity and get all archive file of it
        std::vector<SFile*>& files = archiveFilesPerPopularity[popularityIdxRNG(mSim->mRNGEngine)];

        //create random file selector
        std::uniform_int_distribution<std::size_t> fileIdxRNG(0, files.size() - 1);

        assert(!files.empty());

        //get a random file
        SFile* srcFile = files[fileIdxRNG(mSim->mRNGEngine)];

        if (!hotStorageFull)
            hotStorageFull = hotStorageElement->CanStoreVolume(srcFile->GetSize());

        SReplica* newReplica;
        if (!hotStorageFull)
        {
            newReplica = hotStorageElement->CreateReplica(srcFile, now);
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
            newReplica = coldStorageElement->CreateReplica(srcFile, now);
            if (!newReplica)
                break;

            coldReplicaCreationLimit -= 1;
        }
        else
            break;

        SReplica* srcReplica = srcFile->GetReplicaByStorageElement(archiveStorageElement);
        assert(srcReplica && srcReplica->IsComplete());

        mTransferMgr->CreateTransfer(srcReplica, newReplica, now);
    }
}

void CHotColdStorageTransferGen::UpdateProductionCampaign(SSiteInfo& siteInfo, TickType now)
{
    UpdateWaitingJobs(siteInfo, now);

    UpdateActiveJobs(siteInfo, now);

    UpdateQueuedJobs(siteInfo, now);

    SubmitNewJobs(siteInfo, now);
}

void CHotColdStorageTransferGen::UpdateWaitingJobs(SSiteInfo& siteInfo, TickType now)
{
    std::list<std::unique_ptr<SJobInfo>>& waitingJobs = siteInfo.mWaitingJobs;
    std::list<std::unique_ptr<SJobInfo>>& queuedJobs = siteInfo.mQueuedJobs;
    auto& waitingForSameFile = siteInfo.mWaitingForSameFile;

    CStorageElement* archiveStorageElement = siteInfo.mArchiveToColdLink->GetSrcStorageElement();
    CStorageElement* coldStorageElement = siteInfo.mColdToHotLink->GetSrcStorageElement();
    CStorageElement* hotStorageElement = siteInfo.mColdToHotLink->GetDstStorageElement();

    auto jobIt = waitingJobs.begin();
    while (jobIt != waitingJobs.end())
    {
        SFile* file = (*jobIt)->mInputFile;
        SReplica*& newReplica = (*jobIt)->mInputReplica;
        if (!hotStorageElement->CanStoreVolume(file->GetSize()))
            break;

        CreateJobInputTransfer(archiveStorageElement, coldStorageElement, hotStorageElement, jobIt->get(), now);

        // queue all other jobs that have been waiting for this replica
        auto findResult = waitingForSameFile.find(file);
        if (findResult != waitingForSameFile.end())
        {
            for (auto& it : findResult->second)
            {
                if (it == jobIt)
                    continue;

                newReplica->mUsageCounter += 1;
                (*it)->mInputReplica = newReplica;
                (*it)->mQueuedAt = now;
                queuedJobs.splice(queuedJobs.end(), waitingJobs, it);
            }

            waitingForSameFile.erase(findResult);
        }

        auto tmpIt = jobIt++;
        queuedJobs.splice(queuedJobs.end(), waitingJobs, tmpIt);
    }
}

void CHotColdStorageTransferGen::UpdateActiveJobs(SSiteInfo& siteInfo, TickType now)
{
    const TickType tDelta = now - mLastUpdateTime;

    std::size_t& numRunningJobs = siteInfo.mNumRunningJobs;
    auto& runningJobs = siteInfo.mRunningJobs;

    std::list<std::unique_ptr<SJobInfo>>& activeJobs = siteInfo.mActiveJobs;

    CNetworkLink* hotToCPULink = siteInfo.mHotToCPULink;
    CNetworkLink* cpuToOutputLink = siteInfo.mCPUToOutputLink;

    CStorageElement* hotStorageElement = siteInfo.mColdToHotLink->GetDstStorageElement();

    std::unique_ptr<IInsertValuesContainer> inputTraceInsertQueries = mInputTraceInsertQuery->CreateValuesContainer(9 * 30);
    std::unique_ptr<IInsertValuesContainer> jobTraceInsertQueries = mJobTraceInsertQuery->CreateValuesContainer(6 * 30);
    std::unique_ptr<IInsertValuesContainer> outputTraceInsertQueries = mOutputTraceInsertQuery->CreateValuesContainer(9 * 30);

    //first put all completed jobs back into jobInfos
    auto runningJobsIt = runningJobs.begin();
    while (runningJobsIt != runningJobs.end())
    {
        if (runningJobsIt->first > now)
            break;

        assert(numRunningJobs >= runningJobsIt->second.size());

        numRunningJobs -= runningJobsIt->second.size();
        activeJobs.splice(activeJobs.end(), runningJobsIt->second);

        runningJobsIt = runningJobs.erase(runningJobsIt);
    }

    //should firstly update all mNumActiveTransfers and then calculate bytesDownloaded/uploaded
    const SpaceType bytesDownloaded = (hotToCPULink->mBandwidthBytesPerSecond / (double)(hotToCPULink->mNumActiveTransfers + 1)) * tDelta;

    auto activeJobIt = activeJobs.begin();
    while (activeJobIt != activeJobs.end())
    {
        std::unique_ptr<SJobInfo>& job = *activeJobIt;
        SFile* inputFile = job->mInputFile;
        std::vector<SReplica*>& outputReplicas = job->mOutputReplicas;
        if (job->mCurInputFileSize == 0)
        {
            //download just started
            job->mCurInputFileSize += 1;
            job->mLastTime = now;
            hotToCPULink->mNumActiveTransfers += 1;
        }
        else if (job->mCurInputFileSize < inputFile->GetSize())
        {
            //still downloading input

            SpaceType newSize = job->mCurInputFileSize + bytesDownloaded;
            if (newSize >= inputFile->GetSize())
            {
                //download completed
                hotToCPULink->mUsedTraffic += inputFile->GetSize() - job->mCurInputFileSize;
                hotToCPULink->mNumActiveTransfers -= 1;
                hotToCPULink->mNumDoneTransfers += 1;

                job->mCurInputFileSize = inputFile->GetSize();
                job->mInputReplica->mUsageCounter -= 1;

                if(job->mInputReplica->mUsageCounter == 0)
                {
                    auto res = siteInfo.mUnusedHotReplicasByPopularity.insert({inputFile->mPopularity, SIndexedReplicas()});
                    bool wasAdded = res.first->second.AddReplica(job->mInputReplica);
                    assert(wasAdded);
                }

                inputTraceInsertQueries->AddValue(GetNewId());
                inputTraceInsertQueries->AddValue(job->mJobId);
                inputTraceInsertQueries->AddValue(hotStorageElement->GetSite()->GetId());
                inputTraceInsertQueries->AddValue(hotStorageElement->GetId());
                inputTraceInsertQueries->AddValue(inputFile->GetId());
                inputTraceInsertQueries->AddValue(job->mInputReplica->GetId());
                inputTraceInsertQueries->AddValue(job->mLastTime);
                inputTraceInsertQueries->AddValue(now);
                inputTraceInsertQueries->AddValue(inputFile->GetSize());

                TickType finishTime = now + (static_cast<TickType>(siteInfo.mJobDurationGen->GetValue(mSim->mRNGEngine)) * 60);

                //insert job trace directly so that jobId of input trace points to a valid jobId
                jobTraceInsertQueries->AddValue(job->mJobId);
                jobTraceInsertQueries->AddValue(hotStorageElement->GetSite()->GetId());
                jobTraceInsertQueries->AddValue(job->mCreatedAt);
                jobTraceInsertQueries->AddValue(job->mQueuedAt);
                jobTraceInsertQueries->AddValue(now);
                jobTraceInsertQueries->AddValue(finishTime);

                job->mLastTime = finishTime;

                //move job from jobInfos list to mRunningJobs while it is 'idling'
                runningJobsIt = runningJobs.begin();
                while (runningJobsIt != runningJobs.end())
                {
                    if (runningJobsIt->first == finishTime)
                    {
                        runningJobsIt->second.emplace_back(std::move(job));
                        break;
                    }
                    else if (runningJobsIt->first > finishTime)
                    {
                        runningJobs.emplace(runningJobsIt, finishTime, std::list<std::unique_ptr<SJobInfo>>())->second.emplace_back(std::move(job));
                        break;
                    }

                    ++runningJobsIt;
                }

                if (runningJobsIt == runningJobs.end())
                    runningJobs.emplace_back(finishTime, std::list<std::unique_ptr<SJobInfo>>()).second.emplace_back(std::move(job));

                activeJobIt = activeJobs.erase(activeJobIt);
                numRunningJobs += 1;
                continue;
            }
            else
            {
                hotToCPULink->mUsedTraffic += bytesDownloaded;
                job->mCurInputFileSize = newSize;
            }
        }
        else if (outputReplicas.empty() && (now >= job->mLastTime))
        {
            //no upload created yet but job finished
            job->mLastTime = now;

            //create upload
            //todo: consider cpuToOutputLink->mMaxNumActiveTransfers
            std::size_t numOutputReplicas = siteInfo.mNumOutputGen->GetValue(mSim->mRNGEngine);
            for (; numOutputReplicas > 0; --numOutputReplicas)
            {
                SpaceType size = static_cast<SpaceType>(GiB_TO_BYTES(siteInfo.mOutputSizeGen->GetValue(mSim->mRNGEngine)));
                SFile* outputFile = mSim->mRucio->CreateFile(size, now, SECONDS_PER_MONTH * 12);
                outputReplicas.emplace_back(cpuToOutputLink->GetDstStorageElement()->CreateReplica(outputFile, now));
                outputReplicas.back()->mUsageCounter += 1;
                cpuToOutputLink->mNumActiveTransfers += 1;
                assert(outputReplicas.back());
            }
        }
        else if (!outputReplicas.empty())
        {
            //update uploads
            const SpaceType bytesUploaded = (cpuToOutputLink->mBandwidthBytesPerSecond / (double)(cpuToOutputLink->mNumActiveTransfers + 1)) * tDelta;
            std::size_t idx = 0;
            while (idx < outputReplicas.size())
            {
                SReplica* outputReplica = outputReplicas[idx];

                const SpaceType amount = outputReplica->Increase(bytesUploaded, now);
                cpuToOutputLink->mUsedTraffic += amount;

                if (outputReplica->IsComplete())
                {
                    outputReplica->mUsageCounter -= 1;
                    cpuToOutputLink->mNumActiveTransfers -= 1;
                    cpuToOutputLink->mNumDoneTransfers += 1;

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
                activeJobIt = activeJobs.erase(activeJobIt);
                continue;
            }
        }
        activeJobIt++;
    }
}

void CHotColdStorageTransferGen::UpdateQueuedJobs(SSiteInfo& siteInfo, TickType now)
{
    (void)now;
    std::list<std::unique_ptr<SJobInfo>>& queuedJobs = siteInfo.mQueuedJobs;
    std::list<std::unique_ptr<SJobInfo>>& activeJobs = siteInfo.mActiveJobs;
    std::size_t numTotalJobs = activeJobs.size() + siteInfo.mNumRunningJobs;

    auto jobIt = queuedJobs.begin();
    while (jobIt != queuedJobs.end())
    {
        if (numTotalJobs >= siteInfo.mNumCores)
            break;

        if ((*jobIt)->mInputReplica->IsComplete())
        {
            auto tmpIt = jobIt++;
            activeJobs.splice(activeJobs.end(), queuedJobs, tmpIt);
            ++numTotalJobs;
        }
        else
            ++jobIt;
    }
}

void CHotColdStorageTransferGen::SubmitNewJobs(SSiteInfo& siteInfo, TickType now)
{
    CStorageElement* archiveStorageElement = siteInfo.mArchiveToColdLink->GetSrcStorageElement();
    CStorageElement* coldStorageElement = siteInfo.mColdToHotLink->GetSrcStorageElement();
    CStorageElement* hotStorageElement = siteInfo.mColdToHotLink->GetDstStorageElement();
    SpaceType hotStorageNeeded = 0;

    std::vector<std::vector<SFile*>>& archiveFilesPerPopularity = siteInfo.mArchiveFilesPerPopularity;
    std::unordered_set<SReplica*>& hotReplicaDeletions = siteInfo.mHotReplicaDeletions;

    std::list<std::unique_ptr<SJobInfo>>& activeJobs = siteInfo.mActiveJobs;
    std::list<std::unique_ptr<SJobInfo>>& queuedJobs = siteInfo.mQueuedJobs;
    std::list<std::unique_ptr<SJobInfo>>& waitingJobs = siteInfo.mWaitingJobs;
    auto& waitingForSameFile = siteInfo.mWaitingForSameFile;
    std::list<std::unique_ptr<SJobInfo>> newJobs;

    const std::size_t numCoresInUse = activeJobs.size() + siteInfo.mNumRunningJobs;
    if ((waitingJobs.size() + numCoresInUse) > (2 * siteInfo.mNumCores))
        return;

    assert(siteInfo.mNumCores >= numCoresInUse);

    std::size_t jobCreationLimit = std::min(siteInfo.mNumCores - numCoresInUse, siteInfo.mCoreFillRate);
    if (waitingJobs.size() > siteInfo.mCoreFillRate)
        jobCreationLimit = std::min(jobCreationLimit, (std::size_t)2);

    std::discrete_distribution<std::size_t> popularityIdxRNG = GetPopularityIdxRNG(siteInfo);

    for (; jobCreationLimit > 0; --jobCreationLimit)
    {
        std::vector<SFile*>& files = archiveFilesPerPopularity[popularityIdxRNG(mSim->mRNGEngine)];
        std::uniform_int_distribution<std::size_t> fileIdxRNG(0, files.size() - 1);

        SFile* inputFile = files[fileIdxRNG(mSim->mRNGEngine)];
        SReplica* inputReplica = inputFile->GetReplicaByStorageElement(hotStorageElement);
        std::size_t retry=0;
        while((retry<5) && (hotReplicaDeletions.count(inputReplica) > 0))
        {
            assert(inputReplica); //nullptr must not be in hotReplicaDeletions
            inputFile = files[fileIdxRNG(mSim->mRNGEngine)];
            inputReplica = inputFile->GetReplicaByStorageElement(hotStorageElement);
            ++retry;
        }

        if(retry >= 5)
            continue;

        std::unique_ptr<SJobInfo> newJob = std::make_unique<SJobInfo>();

        newJob->mJobId = GetNewId();
        newJob->mCreatedAt = now;
        newJob->mInputFile = inputFile;
        newJob->mInputReplica = inputReplica;
        if (inputReplica)
        {
            //input replica exists already -> put job into queue
            if(inputReplica->mUsageCounter == 0)
            {
                auto res = siteInfo.mUnusedHotReplicasByPopularity.find(inputFile->mPopularity);
                if(res != siteInfo.mUnusedHotReplicasByPopularity.end())
                    res->second.RemoveReplica(inputReplica);
            }
            inputReplica->mUsageCounter += 1;
            newJob->mQueuedAt = now;
            queuedJobs.emplace_back(std::move(newJob));
        }
        else
        {
            //input replica not there -> sum needed storage space and handle after jobs have been created
            hotStorageNeeded += inputFile->GetSize();
            newJobs.emplace_back(std::move(newJob));
        }
    }

    if (!hotStorageElement->CanStoreVolume(hotStorageNeeded))
    {
        //not enough storage for all new jobs -> try to free storage and put jobs into waitForStorage list
        std::map<std::uint32_t, SIndexedReplicas>& unusedHotReplicasByPopularity = siteInfo.mUnusedHotReplicasByPopularity;
        std::vector<SReplica*> replicasToRemove; //need to cache replicas because removing invalidates popularityIt
        auto popularityIt = unusedHotReplicasByPopularity.begin();
        while ((hotStorageNeeded > 0) && (popularityIt != unusedHotReplicasByPopularity.end()))
        {
            SIndexedReplicas& indexedReplicas = popularityIt->second;
            while ((hotStorageNeeded > 0) && !indexedReplicas.IsEmpty())
            {
                SReplica* replica = indexedReplicas.ExtractBack();

                assert(replica->mUsageCounter == 0);
                assert(replica->IsComplete());

                replicasToRemove.push_back(replica);

                hotStorageNeeded = (replica->GetCurSize() <= hotStorageNeeded) ? (hotStorageNeeded - replica->GetCurSize()) : 0;
            }
            ++popularityIt;
        }

        for(SReplica* replica : replicasToRemove)
        {
            SFile* file = replica->GetFile();

            //create transfer to remove replicas or remove replica instantly
            if (!file->GetReplicaByStorageElement(coldStorageElement))
            {
                SReplica* dstReplica = coldStorageElement->CreateReplica(file, now);
                assert(dstReplica);
                mTransferMgr->CreateTransfer(replica, dstReplica, now, true);

                //prevent using this replica when creating new jobs
                hotReplicaDeletions.insert(replica);
            }
            else
                hotStorageElement->RemoveReplica(replica, now);
            
            //no need to decrease usage counter it's already 0
        }

        for (auto it = newJobs.begin(); it != newJobs.end(); ++it)
            waitingForSameFile[(*it)->mInputFile].push_back(it);
        waitingJobs.splice(waitingJobs.end(), newJobs);
    }
    else
    {
        //enough storage available -> create replicas + transfers and put jobs into queue
        for (std::unique_ptr<SJobInfo>& job : newJobs)
        {
            CreateJobInputTransfer(archiveStorageElement, coldStorageElement, hotStorageElement, job.get(), now);

            SReplica* newReplica = job->mInputReplica;
            SFile* file = job->mInputFile;

            //also queue all jobs that have been waiting for the same file
            auto findResult = waitingForSameFile.find(file);
            if (findResult != waitingForSameFile.end())
            {
                for (auto it : findResult->second)
                {
                    newReplica->mUsageCounter += 1;
                    (*it)->mInputReplica = newReplica;
                    (*it)->mQueuedAt = now;
                    queuedJobs.splice(queuedJobs.end(), waitingJobs, it);
                }

                waitingForSameFile.erase(findResult);
            }
        }
        queuedJobs.splice(queuedJobs.end(), newJobs);
    }
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
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

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



CJobIOTransferGen::CJobIOTransferGen(IBaseSim* sim,
                                    std::shared_ptr<CTransferManager> transferMgr,
                                    TickType tickFreq,
                                    TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTransferMgr(transferMgr),
      mTickFreq(tickFreq),
      mLastUpdateTime(startTick)
{
    mInputTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY InputTraces(id, jobId, siteId, storageElementId, fileId, replicaId, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 9, '?');
    mJobTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY JobTraces(id, siteId, createdAt, queuedAt, startedAt, finishedAt) FROM STDIN with(FORMAT csv);", 6, '?');
    mOutputTraceInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY OutputTraces(id, jobId, siteId, storageElementId, fileId, replicaId, startedAt, finishedAt, traffic) FROM STDIN with(FORMAT csv);", 9, '?');
}

void CJobIOTransferGen::OnUpdate(TickType now)
{
    assert(now >= mLastUpdateTime);

    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    const TickType tDelta = now - mLastUpdateTime;
    mLastUpdateTime = now;
    
    std::unique_ptr<IInsertValuesContainer> inputTraceInsertQueries = mInputTraceInsertQuery->CreateValuesContainer(9 * 30);
    std::unique_ptr<IInsertValuesContainer> jobTraceInsertQueries = mJobTraceInsertQuery->CreateValuesContainer(6 * 30);
    std::unique_ptr<IInsertValuesContainer> outputTraceInsertQueries = mOutputTraceInsertQuery->CreateValuesContainer(9 * 30);
    RNGEngineType& rngEngine = mSim->mRNGEngine;
    
    for(SSiteInfo& siteInfo : mSiteInfos)
    {
        std::list<std::unique_ptr<SJobInfo>>& activeJobs = siteInfo.mActiveJobs;

        auto& runningJobs = siteInfo.mRunningJobs;
        std::size_t& numRunningJobs = siteInfo.mNumRunningJobs;

        CNetworkLink* diskToCPULink = siteInfo.mDiskToCPULink;
        CNetworkLink* cpuToOutputLink = siteInfo.mCPUToOutputLink;
        CNetworkLink* cloudToDiskLink = siteInfo.mCloudToDiskLink;

        CStorageElement* diskSE = diskToCPULink->GetSrcStorageElement();
        CStorageElement* outputSE = cpuToOutputLink->GetDstStorageElement();

        //first put all completed jobs back into jobInfos
        auto runningJobsIt = runningJobs.begin();
        while (runningJobsIt != runningJobs.end())
        {
            if (runningJobsIt->first > now)
                break;
            
            assert(numRunningJobs >= runningJobsIt->second.size());

            numRunningJobs -= runningJobsIt->second.size();
            activeJobs.splice(activeJobs.end(), runningJobsIt->second);

            runningJobsIt = runningJobs.erase(runningJobsIt);
        }

        //should firstly update all mNumActiveTransfers and then calculate bytesDownloaded/uploaded
        const SpaceType bytesDownloaded = (diskToCPULink->mBandwidthBytesPerSecond / (double)(diskToCPULink->mNumActiveTransfers+1)) * tDelta;

        auto activeJobIt = activeJobs.begin();
        while(activeJobIt != activeJobs.end())
        {
            std::unique_ptr<SJobInfo>& job = *activeJobIt;
            SFile* inputFile = job->mInputFile;
            std::vector<SReplica*>& outputReplicas = job->mOutputReplicas;
            if (job->mCurInputFileSize == 0)
            {
                //download just started
                job->mCurInputFileSize += 1;
                job->mStartedAt = now;
                diskToCPULink->mNumActiveTransfers += 1;
            }
            else if(job->mCurInputFileSize < inputFile->GetSize())
            {
                //still downloading input

                SpaceType newSize = job->mCurInputFileSize + bytesDownloaded;
                if(newSize >= inputFile->GetSize())
                {
                    //download completed
                    diskToCPULink->mUsedTraffic += inputFile->GetSize() - job->mCurInputFileSize;
                    job->mCurInputFileSize = inputFile->GetSize();
                    diskToCPULink->mNumActiveTransfers -= 1;
                    diskToCPULink->mNumDoneTransfers += 1;

                    inputTraceInsertQueries->AddValue(GetNewId());
                    inputTraceInsertQueries->AddValue(job->mJobId);
                    inputTraceInsertQueries->AddValue(diskSE->GetSite()->GetId());
                    inputTraceInsertQueries->AddValue(diskSE->GetId());
                    inputTraceInsertQueries->AddValue(inputFile->GetId());
                    //ensure src replica stays available
                    IdType srcReplicaId = inputFile->GetReplicaByStorageElement(diskSE)->GetId();
                    inputTraceInsertQueries->AddValue(srcReplicaId);
                    inputTraceInsertQueries->AddValue(job->mStartedAt);
                    inputTraceInsertQueries->AddValue(now);
                    inputTraceInsertQueries->AddValue(inputFile->GetSize());

                    TickType finishTime = now + (static_cast<TickType>(siteInfo.mJobDurationGen->GetValue(rngEngine)) * 60);

                    //insert job trace directly so that jobId of input trace points to a valid jobId
                    jobTraceInsertQueries->AddValue(job->mJobId);
                    jobTraceInsertQueries->AddValue(diskSE->GetSite()->GetId());
                    jobTraceInsertQueries->AddValue(job->mStartedAt);
                    jobTraceInsertQueries->AddValue(job->mStartedAt);
                    jobTraceInsertQueries->AddValue(now);
                    jobTraceInsertQueries->AddValue(finishTime);

                    job->mStartedAt = now;
                    job->mFinishedAt = finishTime;

                    //move job from jobInfos list to mRunningJobs while it is 'idling'
                    runningJobsIt = runningJobs.begin();
                    while (runningJobsIt != runningJobs.end())
                    {
                        if (runningJobsIt->first == finishTime)
                        {
                            runningJobsIt->second.emplace_back(std::move(job));
                            break;
                        }
                        else if(runningJobsIt->first > finishTime)
                        {
                            runningJobs.emplace(runningJobsIt, finishTime, std::list<std::unique_ptr<SJobInfo>>())->second.emplace_back(std::move(job));
                            break;
                        }

                        ++runningJobsIt;
                    }

                    if (runningJobsIt == runningJobs.end())
                        runningJobs.emplace_back(finishTime, std::list<std::unique_ptr<SJobInfo>>()).second.emplace_back(std::move(job));

                    activeJobIt = activeJobs.erase(activeJobIt);
                    numRunningJobs += 1;
                    continue;
                }
                else
                {
                    diskToCPULink->mUsedTraffic += bytesDownloaded;
                    job->mCurInputFileSize = newSize;
                }
            }
            else if(outputReplicas.empty() && (now >= job->mFinishedAt))
            {
                //no upload created yet but job finished

                //create upload
                //todo: consider cpuToOutputLink->mMaxNumActiveTransfers
                std::size_t numOutputReplicas = siteInfo.mNumOutputGen->GetValue(rngEngine);
                for(;numOutputReplicas>0; --numOutputReplicas)
                {
                    SpaceType size = static_cast<SpaceType>(GiB_TO_BYTES(siteInfo.mOutputSizeGen->GetValue(rngEngine)));
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

                        outputTraceInsertQueries->AddValue(GetNewId());
                        outputTraceInsertQueries->AddValue(job->mJobId);
                        outputTraceInsertQueries->AddValue(outputReplica->GetStorageElement()->GetSite()->GetId());
                        outputTraceInsertQueries->AddValue(outputReplica->GetStorageElement()->GetId());
                        outputTraceInsertQueries->AddValue(outputReplica->GetFile()->GetId());
                        outputTraceInsertQueries->AddValue(outputReplica->GetId());
                        outputTraceInsertQueries->AddValue(job->mFinishedAt);
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

                if(outputReplicas.empty())
                {
                    //SReplica* inputSrcReplica = inputFile->GetReplicaByStorageElement(diskSE);
                    //assert(inputSrcReplica);
                    //if (inputSrcReplica->mNumStagedIn >= inputFile->mPopularity)
                        //diskSE->RemoveReplica(inputSrcReplica, now);

                    activeJobIt = activeJobs.erase(activeJobIt);
                    continue;
                }
            }
            activeJobIt++;
        }

        assert(siteInfo.mNumCores >= (activeJobs.size() + numRunningJobs));

        std::size_t numJobsToCreate = std::min(siteInfo.mNumCores - (activeJobs.size() + numRunningJobs), siteInfo.mCoreFillRate);
        for(const std::unique_ptr<SReplica>& replica : diskSE->GetReplicas()) // consider staging in the same replica multiple times
        {
            //create new jobs from diskSE replicas
            if(numJobsToCreate == 0)
                break;
            if(!replica->IsComplete())
                continue;
            //if(replica->mNumStagedIn >= replica->GetFile()->mPopularity)
                //continue;
            
            std::unique_ptr<SJobInfo>& newJobInfo = activeJobs.emplace_front(std::make_unique<SJobInfo>());
            newJobInfo->mJobId = GetNewId();
            newJobInfo->mInputFile = replica->GetFile();
            
            //replica->mNumStagedIn += 1;
            numJobsToCreate -= 1;
        }

        if(diskSE->GetUsedStorageQuotaRatio() <= siteInfo.mDiskQuotaThreshold)
        {
            //if storage on diskSE available create transfers from cloudSE to diskSE
            assert(cloudToDiskLink->mMaxNumActiveTransfers >= cloudToDiskLink->mNumActiveTransfers);
            std::size_t numTransfers = (cloudToDiskLink->mMaxNumActiveTransfers - cloudToDiskLink->mNumActiveTransfers) / 2.0;
            SpaceType volumeSum = 0;
            for(const std::unique_ptr<SReplica>& replica : cloudToDiskLink->GetSrcStorageElement()->GetReplicas())
            {
                if (!replica->IsComplete())
                    continue;

                volumeSum += replica->GetFile()->GetSize();
                if(numTransfers == 0 || !diskSE->CanStoreVolume(volumeSum))
                    break;

                SReplica* newReplica = diskSE->CreateReplica(replica->GetFile(), now);
                if (!newReplica)
                {
                    volumeSum -= replica->GetFile()->GetSize();
                    continue;
                }

                mTransferMgr->CreateTransfer(replica.get(), newReplica, now, false);
                numTransfers -= 1;
            }
        }
    }
    COutput::GetRef().QueueInserts(std::move(inputTraceInsertQueries));
    COutput::GetRef().QueueInserts(std::move(jobTraceInsertQueries));
    COutput::GetRef().QueueInserts(std::move(outputTraceInsertQueries));
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