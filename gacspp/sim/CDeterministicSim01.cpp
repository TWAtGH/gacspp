#include <forward_list>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>
#include <sstream>

#include "CDeterministicSim01.hpp"

#include "infrastructure/CRucio.hpp"
#include "infrastructure/CStorageElement.hpp"
#include "infrastructure/SFile.hpp"

#include "scheduleables/CommonScheduleables.hpp"
#include "scheduleables/TransferManager.hpp"
#include "scheduleables/TransferGenerators.hpp"

#include "output/COutput.hpp"

#include "third_party/nlohmann/json.hpp"


class CDeterministicTransferGen : public CScheduleable, public CBaseOnDeletionInsert
{
private:
    IBaseSim* mSim;

    std::shared_ptr<CTransferBatchManager> mTransferMgr;

    std::string mFilePathTmpl;
    std::uint32_t mCurFileIdx;
    std::ifstream mDataFile;

    std::ifstream::char_type mTypeJobDelim;
    std::ifstream::char_type mTypeInputDelim;
    std::ifstream::char_type mTypeOutputDelim;
    std::ifstream::char_type mLineDelim;

    struct SJob
    {
        TickType mStageInDuration;
        TickType mStageOutDuration;
        TickType mJobEndTime;
        std::vector<std::pair<std::uint64_t, SpaceType>> mInputFiles;
        std::vector<std::pair<std::uint64_t, SpaceType>> mOutputFiles;

        std::shared_ptr<CTransferBatchManager::STransferBatch> mTransferBatch;
    };

    typedef std::pair<TickType, std::forward_list<SJob>> JobBatchType;

    std::list<JobBatchType> mJobBatchList;
    std::list<JobBatchType>::iterator mCurJobBatch;

    std::list<std::shared_ptr<SReplica>> mTmpReplicas;

    std::vector<std::size_t> mFileIndices;

    bool LoadNextFile()
    {
        constexpr char sym = '$';

        const std::size_t start = mFilePathTmpl.find_first_of(sym);
        const std::size_t end = mFilePathTmpl.find_last_of(sym) + 1;
        assert((start != std::string::npos) && (end > start));

        std::stringstream filePathBuilder;
        filePathBuilder << std::setfill('0');
        filePathBuilder << mFilePathTmpl.substr(0, start);
        filePathBuilder << std::setw(end-start) << (mCurFileIdx++);
        filePathBuilder << mFilePathTmpl.substr(end, std::string::npos);

        mDataFile = std::ifstream(filePathBuilder.str());

        std::cout<<"Loading: "<<filePathBuilder.str()<<" - "<<mDataFile.is_open()<<std::endl;

        if(mDataFile.is_open())
        {
            mTypeJobDelim = mDataFile.widen('j');
            mTypeInputDelim = mDataFile.widen('i');
            mTypeOutputDelim = mDataFile.widen('o');
        }

        return mDataFile.is_open();
    }

    bool ReadNextJobBatch()
    {
        if(!mDataFile.is_open() || mDataFile.eof())
            if(!LoadNextFile())
                return false;

        JobBatchType& newJobBatch = mJobBatchList.emplace_front();
        mCurJobBatch = mJobBatchList.begin();

        mDataFile >> newJobBatch.first;
        mDataFile.ignore(1);

        TickType stageInDuration, jobDuration, stageOutDuration;
        while(mDataFile.peek() == mTypeJobDelim)
        {
            mDataFile.ignore(2); // "j,"

            mDataFile >> stageInDuration;
            mDataFile.ignore(1);
            stageInDuration = std::max<TickType>(1, stageInDuration);

            mDataFile >> jobDuration;
            mDataFile.ignore(1);

            const TickType jobEndTime = newJobBatch.first + stageInDuration + jobDuration;

            mDataFile >> stageOutDuration;
            mDataFile.ignore(1);
            stageOutDuration = std::max<TickType>(1, stageOutDuration);

            auto prev = newJobBatch.second.before_begin();
            auto cur = newJobBatch.second.begin();
            while((cur != newJobBatch.second.end()) && (cur->mJobEndTime < jobEndTime))
            {
                prev = cur;
                ++cur;
            }

            cur = newJobBatch.second.emplace_after(prev);
            cur->mStageInDuration = stageInDuration;
            cur->mStageOutDuration = stageOutDuration;
            cur->mJobEndTime = jobEndTime;

            std::ifstream::char_type type = mDataFile.peek();
            while(type == mTypeInputDelim || type == mTypeOutputDelim)
            {
                mDataFile.ignore(2); // "i," or "o,"

                std::pair<std::uint64_t, SpaceType> file;

                mDataFile>>file.first;
                mDataFile.ignore(1);

                mDataFile >> file.second;
                mDataFile.ignore(1); // ',' or '\n'

                if(type == mTypeInputDelim)
                    cur->mInputFiles.push_back(std::move(file));
                else
                    cur->mOutputFiles.push_back(std::move(file));

                type = mDataFile.peek();
            }
        }

        return true;
    }

    std::shared_ptr<IPreparedInsert> mFileInsertQuery;
    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

public:
    std::chrono::duration<double> mUpdateCleanDurationSummed = std::chrono::duration<double>::zero();
    std::chrono::duration<double> mUpdateInDurationSummed = std::chrono::duration<double>::zero();
    std::chrono::duration<double> mUpdateOutDurationSummed = std::chrono::duration<double>::zero();

    std::vector<std::pair<CStorageElement*, CStorageElement*>> mTapeStorageElements;
    CStorageElement* mComputingStorageElement = nullptr;

    CDeterministicTransferGen(IBaseSim* sim,
                              std::shared_ptr<CTransferBatchManager> transferMgr,
                              const std::string& filePathTmpl,
                              const std::uint32_t startFileIdx=0)
        : mSim(sim), mTransferMgr(transferMgr),
          mFilePathTmpl(filePathTmpl), mCurFileIdx(startFileIdx)
    {
        bool ok = ReadNextJobBatch();
        assert(ok);
        mNextCallTick = mCurJobBatch->first;
        mSim->mRucio->mFiles.reserve(12000000);
        mFileIndices.resize(12000000, 12000000);
    }

    void InsertTmpReplica(std::shared_ptr<SReplica> replica)
    {
        auto tmpReplicaIt = mTmpReplicas.begin();
        while (tmpReplicaIt != mTmpReplicas.end())
        {
            if ((*tmpReplicaIt)->mExpiresAt <= replica->mExpiresAt)
            {
                mTmpReplicas.insert(tmpReplicaIt, std::move(replica));
                return;
            }
            ++tmpReplicaIt;
        }
        if (tmpReplicaIt == mTmpReplicas.end())
            mTmpReplicas.emplace_back(replica);
    }

    void CleanupTmpReplicas(const TickType now)
    {
        std::vector<std::shared_ptr<SReplica>> expiredReplicas;
        while (!mTmpReplicas.empty())
        {
            std::shared_ptr<SReplica>& replica = mTmpReplicas.back();
            if (now < replica->mExpiresAt)
            {
                mNextCallTick = std::min(mNextCallTick, replica->mExpiresAt);
                break;
            }

            std::vector<std::shared_ptr<SReplica>>& fileReplicas = replica->GetFile()->mReplicas;
            replica->OnRemoveByFile(now);
            expiredReplicas.emplace_back(replica);
            for(std::size_t i=0; i<fileReplicas.size(); ++i)
            {
                if(fileReplicas[i] == replica)
                {
                    fileReplicas[i] = std::move(fileReplicas.back());
                    fileReplicas.pop_back();
                    break;
                }
            }
            mTmpReplicas.pop_back();
        }

        if (expiredReplicas.empty())
            return;

        /*auto& replicaListeners = mSim->mRucio->mReplicaActionListeners;
        if (!replicaListeners.empty())
        {
            //put files in continous mem in form of weak_ptr
            std::vector<std::weak_ptr<SReplica>> removedReplicas;
            removedReplicas.reserve(expiredReplicas.size());
            for (std::shared_ptr<SReplica>& replica : expiredReplicas)
                removedReplicas.emplace_back(replica);

            //notify all listeners
            for (std::size_t i = 0; i < replicaListeners.size();)
            {
                std::shared_ptr<IReplicaActionListener> listener = replicaListeners[i].lock();
                if (!listener)
                {
                    //remove invalid listeners
                    replicaListeners[i] = std::move(replicaListeners.back());
                    replicaListeners.pop_back();
                    continue;
                }

                listener->OnReplicasDeleted(now, removedReplicas);

                ++i;
            }
        }*/
    }

    void UpdateTmpReplicaExpiresAt(std::shared_ptr<SReplica> replica, const TickType newExpiresAt)
    {
        const TickType oldExpiresAt = replica->mExpiresAt;
        replica->ExtendExpirationTime(newExpiresAt);

        auto revIt = mTmpReplicas.rbegin();
        auto curReplicaPos = mTmpReplicas.end();
        while (revIt != mTmpReplicas.rend())
        {
            if ((*revIt)->mExpiresAt == oldExpiresAt)
            {
                if ((*revIt) == replica)
                {
                    ++revIt;
                    curReplicaPos = revIt.base();
                    break;
                }
            }
            else if ((*revIt)->mExpiresAt > oldExpiresAt)
                break;
            ++revIt;
        }

        while (revIt != mTmpReplicas.rend())
        {
            if ((*revIt)->mExpiresAt > replica->mExpiresAt)
                break;
            ++revIt;
        }

        mTmpReplicas.emplace(revIt.base(), std::move(replica));

        if (curReplicaPos != mTmpReplicas.end())
            mTmpReplicas.erase(curReplicaPos);
    }

    void StageOut(const TickType now)
    {
        for (auto jobBatchIt = mJobBatchList.begin(); jobBatchIt != mJobBatchList.end(); ++jobBatchIt)
        {
            auto& jobBatch = jobBatchIt->second;
            while (!jobBatch.empty())
            {
                SJob& job = jobBatch.front();
                if (now < job.mJobEndTime)
                {
                    mNextCallTick = std::min(mNextCallTick, job.mJobEndTime);
                    break;
                }

                for (const std::pair<std::uint64_t, SpaceType>& outFile : job.mOutputFiles)
                {
                    assert(mFileIndices[outFile.first] == mFileIndices.size());

                    mFileIndices[outFile.first] = mSim->mRucio->mFiles.size();
                    std::shared_ptr<SFile> file = mSim->mRucio->CreateFile(outFile.second, now, SECONDS_PER_MONTH * 13);
                    std::shared_ptr<SReplica> srcReplica = mComputingStorageElement->CreateReplica(file, now);

                    assert(srcReplica);

                    srcReplica->Increase(outFile.second, now);
                    srcReplica->mExpiresAt = job.mJobEndTime + job.mStageOutDuration + 120;

                    InsertTmpReplica(srcReplica);

                    //TODO: create transfer for stageout to a storage element
                    //std::shared_ptr<SReplica> r = mDiskStorageElement->CreateReplica(file, now);
                    //mTransferMgr->CreateTransfer(srcReplica, r, now, 0, job.mStageOutDuration);
                }
                jobBatch.pop_front();
            }

            if (jobBatch.empty())
            {
                jobBatchIt = mJobBatchList.erase(jobBatchIt);
                --jobBatchIt;
            }
        }
    }

    void StageIn(const TickType now)
    {
        if (mCurJobBatch == mJobBatchList.end())
            return;

        if (now < mCurJobBatch->first)
        {
            mNextCallTick = std::min(mNextCallTick, mCurJobBatch->first);
            return;
        }

        for (SJob& job : mCurJobBatch->second)
        {
            job.mTransferBatch = std::make_shared<CTransferBatchManager::STransferBatch>();
            std::pair<CStorageElement*, CStorageElement*> tapeDiskStorageElement{nullptr, nullptr};
            for (const std::pair<std::uint64_t, SpaceType>& inFile : job.mInputFiles)
            {
                std::shared_ptr<SFile> file;
                if(mFileIndices[inFile.first] == mFileIndices.size())
                {
                    mFileIndices[inFile.first] = mSim->mRucio->mFiles.size();
                    file = mSim->mRucio->CreateFile(inFile.second, now, SECONDS_PER_MONTH * 13);
                }
                else if(!tapeDiskStorageElement.first)
                {
                    file = mSim->mRucio->mFiles[mFileIndices[inFile.first]];

                    for(const std::pair<CStorageElement*, CStorageElement*>& tapeDisk : mTapeStorageElements)
                    {
                        if(file->GetReplicaByStorageElement(tapeDisk.first))
                        {
                            tapeDiskStorageElement = tapeDisk;
                            break;
                        }
                    }
                }
            }

            if(!tapeDiskStorageElement.first)
            {
                //select tapeDiskStorageElement
            }

            job.mTransferBatch->mStartAt = now + 2700;
            job.mTransferBatch->mRoute.push_back(tapeDiskStorageElement.first->GetNetworkLink(tapeDiskStorageElement.second));
            job.mTransferBatch->mRoute.push_back(tapeDiskStorageElement.second->GetNetworkLink(mComputingStorageElement));

            for (const std::pair<std::uint64_t, SpaceType>& inFile : job.mInputFiles)
            {
                std::shared_ptr<SFile> file = mSim->mRucio->mFiles[mFileIndices[inFile.first]];
                std::shared_ptr<SReplica> srcReplica = file->GetReplicaByStorageElement(tapeDiskStorageElement.first);

                if(!srcReplica)
                {
                    srcReplica = tapeDiskStorageElement.first->CreateReplica(file, now);
                    assert(srcReplica);
                    srcReplica->Increase(inFile.second, now);
                }

                std::shared_ptr<SReplica> dstReplica = tapeDiskStorageElement.second->CreateReplica(file, now);
                assert(dstReplica);

                //dstReplica->mExpiresAt = job.mJobEndTime + 60;
                //InsertTmpReplica(dstReplica);
                job.mTransferBatch->mTransfers.emplace_back(std::make_unique<CTransferBatchManager::STransfer>(srcReplica, dstReplica, now, now+2700));
            }

            mTransferMgr->QueueTransferBatch(std::move(job.mTransferBatch));
        }

        if (!mDataFile.is_open())
        {
            mCurJobBatch = mJobBatchList.end();
            return;
        }

        ReadNextJobBatch();
        mNextCallTick = std::min(mNextCallTick, mCurJobBatch->first);
    }

    void OnUpdate(const TickType now)
    {
        CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

        if (mTmpReplicas.empty() && mJobBatchList.empty() && (mCurJobBatch == mJobBatchList.end()))
        {
            mNextCallTick = now + 20;
            //if ((mTransferMgr->GetNumQueuedTransfers() + mTransferMgr->GetNumActiveTransfers()) == 0)
                //mSim->Stop();
        }
        else
            mNextCallTick = std::numeric_limits<TickType>::max();

        {
            CScopedTimeDiff subDurationUpdate(mUpdateCleanDurationSummed, true);
            CleanupTmpReplicas(now);
        }

        {
            CScopedTimeDiff subDurationUpdate(mUpdateOutDurationSummed, true);
            StageOut(now);
        }

        {
            CScopedTimeDiff subDurationUpdate(mUpdateInDurationSummed, true);
            StageIn(now);
        }
    }

    void Shutdown(const TickType now)
    {
        std::size_t numQueuedStageOut = 0;
        while(!mJobBatchList.empty())
        {
            auto& jobBatch = mJobBatchList.front().second;
            while (!jobBatch.empty())
            {
                numQueuedStageOut += jobBatch.front().mOutputFiles.size();
                jobBatch.pop_front();
            }
            mJobBatchList.pop_front();
        }
        std::cout<<"numQueuedStageOut="<<numQueuedStageOut<<std::endl;
/*
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
        OnReplicasDeleted(now, replicas);*/
    }
};


bool CDeterministicSim01::SetupDefaults(const json& profileJson)
{
    if(!CDefaultBaseSim::SetupDefaults(profileJson))
        return false;

    auto heartbeat = std::make_shared<CHeartbeat>(this, static_cast<std::uint32_t>(SECONDS_PER_DAY), static_cast<TickType>(SECONDS_PER_DAY));

    json transferManagerCfg, transferGenCfg;
    try
    {
        transferManagerCfg = profileJson.at("transferCfgs")[0].at("manager");
        transferGenCfg = profileJson.at("transferCfgs")[0].at("generator");
    }
    catch (const json::out_of_range& error)
    {
        std::cout << "Invalid transfer configuration: " << error.what() << std::endl;
        return false;
    }

    auto transferManager = std::dynamic_pointer_cast<CTransferBatchManager>(CreateTransferManager(transferManagerCfg));
    if (!transferManager)
    {
        std::cout << "Failed creating transfer manager" << std::endl;
        return false;
    }

    heartbeat->mTransferManagers.push_back(transferManager);

    std::shared_ptr<CDeterministicTransferGen> transferGen;
    try
    {
        const std::string fileDataFilePathTmpl = transferGenCfg.at("fileDataFilePathTmpl").get<std::string>();
        const std::uint32_t fileDataFileFirstIdx = transferGenCfg.at("fileDataFileFirstIdx").get<std::uint32_t>();

        transferGen = std::make_shared<CDeterministicTransferGen>(this, transferManager, fileDataFilePathTmpl, fileDataFileFirstIdx);

        for(const json& storageElementName : transferGenCfg.at("diskStorageElements"))
        {
            //CStorageElement* storageElement = mRucio->GetStorageElementByName(storageElementName.get<std::string>());
            //if(storageElement)
                //transferGen->mDiskStorageElement = storageElement;
            //else
                //std::cout<<"Failed to find diskStorageElements: "<<storageElementName.get<std::string>()<<std::endl;
        }

        for(const json& storageElementName : transferGenCfg.at("computingStorageElements"))
        {
            CStorageElement* storageElement = mRucio->GetStorageElementByName(storageElementName.get<std::string>());
            if(storageElement)
                transferGen->mComputingStorageElement = storageElement;
            else
                std::cout<<"Failed to find computingStorageElements: "<<storageElementName.get<std::string>()<<std::endl;
        }

        mRucio->mFileActionListeners.emplace_back(transferGen);
        //mRucio->mReplicaActionListeners.emplace_back(transferGen);
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load deterministic transfer gen cfg: " << error.what() << std::endl;
        return false;
    }

    //std::shared_ptr<CReaperCaller> reaper;
    try
    {
        //const json& reaperCfg = profileJson.at("reaper");
        //const TickType tickFreq = reaperCfg.at("tickFreq").get<TickType>();
        //const TickType startTick = reaperCfg.at("startTick").get<TickType>();
        //reaper = std::make_shared<CReaperCaller>(mRucio.get(), tickFreq, startTick);
    }
    catch (const json::out_of_range& error)
    {
        std::cout << "Failed to load reaper cfg: " << error.what() << std::endl;
        //reaper = std::make_shared<CReaperCaller>(mRucio.get(), 600, 600);
    }
    //heartbeat->mProccessDurations[reaper->mName] = &(reaper->mUpdateDurationSummed);

    heartbeat->mProccessDurations[transferManager->mName] = &(transferManager->mUpdateDurationSummed);
    heartbeat->mProccessDurations[transferGen->mName] = &(transferGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["TransferGenClean"] = &(transferGen->mUpdateCleanDurationSummed);
    heartbeat->mProccessDurations["TransferGenIn"] = &(transferGen->mUpdateInDurationSummed);
    heartbeat->mProccessDurations["TransferGenOut"] = &(transferGen->mUpdateOutDurationSummed);

    mSchedule.push(transferManager);
    mSchedule.push(transferGen);
    //mSchedule.push(reaper);
    mSchedule.push(heartbeat);

    return true;
}
