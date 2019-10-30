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

#include "CommonScheduleables.hpp"

#include "output/COutput.hpp"

#include "third_party/json.hpp"

#define LFN_MAP_IDX_OFFSET (20)
#define NUM_LFN_MAPS (80)

class CDeterministicTransferGen : public CScheduleable, public CBaseOnDeletionInsert
{
private:
    IBaseSim* mSim;

    std::shared_ptr<CFixedTimeTransferManager> mTransferMgr;

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
        TickType mJobDuration;
        TickType mStageOutDuration;
        std::vector<std::pair<std::string, SpaceType>> mInputFiles;
        std::vector<std::pair<std::string, SpaceType>> mOutputFiles;
    };

    typedef std::pair<TickType, std::forward_list<SJob>> JobBatchType;

    std::list<JobBatchType> mJobBatchList;
    std::list<JobBatchType>::iterator mCurJobBatch;

    std::list<std::shared_ptr<SReplica>> mTmpReplicas;

    std::unordered_map<std::string, std::shared_ptr<SFile>> mLFNToFile[NUM_LFN_MAPS+1];

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

        mJobBatchList.emplace_front();
        mCurJobBatch = mJobBatchList.begin();

        mDataFile >> mCurJobBatch->first;
        mDataFile.ignore(1);

        while(mDataFile.peek() == mTypeJobDelim)
        {
            SJob& newJob = mCurJobBatch->second.emplace_front();

            mDataFile.ignore(2); // "j,"

            mDataFile >> newJob.mStageInDuration;
            mDataFile.ignore(1);

            mDataFile >> newJob.mJobDuration;
            mDataFile.ignore(1);

            mDataFile >> newJob.mStageOutDuration;
            mDataFile.ignore(1);

            std::ifstream::char_type type = mDataFile.peek();
            while(type == mTypeInputDelim || type == mTypeOutputDelim)
            {
                mDataFile.ignore(2); // "i," or "o,"

                std::pair<std::string, SpaceType> file;

                std::getline(mDataFile, file.first, ',');

                mDataFile >> file.second;
                mDataFile.ignore(1); // ',' or '\n'

                if(type == mTypeInputDelim)
                    newJob.mInputFiles.push_back(std::move(file));
                else
                    newJob.mOutputFiles.push_back(std::move(file));

                type = mDataFile.peek();
            }
        }

        return true;
    }

    std::shared_ptr<IPreparedInsert> mFileInsertQuery;
    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

public:
    CStorageElement* mDiskStorageElement = nullptr;
    CStorageElement* mComputingStorageElement = nullptr;

    CDeterministicTransferGen(IBaseSim* sim,
                              std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                              const std::string& filePathTmpl,
                              const std::uint32_t startFileIdx=0)
        : mSim(sim), mTransferMgr(transferMgr),
          mFilePathTmpl(filePathTmpl), mCurFileIdx(startFileIdx)
    {
        bool ok = ReadNextJobBatch();
        assert(ok);
        mNextCallTick = mCurJobBatch->first;
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
            replica->GetFile()->ExtractExpiredReplicas(now, expiredReplicas);
            mTmpReplicas.pop_back();
        }

        if (expiredReplicas.empty())
            return;

        auto& replicaListeners = mSim->mRucio->mReplicaActionListeners;
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
        }
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
            {
                const TickType start = jobBatchIt->first;
                auto prevIt = jobBatchIt->second.before_begin();
                auto jobIt = jobBatchIt->second.begin();
                while (jobIt != jobBatchIt->second.end())
                {
                    const TickType jobEndTime = start + jobIt->mStageInDuration + jobIt->mJobDuration;
                    const TickType stageOutEndTime = jobEndTime + jobIt->mStageOutDuration;
                    if (now >= jobEndTime)
                    {
                        for (const std::pair<std::string, SpaceType>& outFile : jobIt->mOutputFiles)
                        {
                            std::shared_ptr<SFile> file;
                            const std::size_t idx = std::clamp<std::size_t>(outFile.first.length(), LFN_MAP_IDX_OFFSET, NUM_LFN_MAPS);
                            auto insertResult = mLFNToFile[idx].emplace(outFile.first, file);

                            if (insertResult.second)
                            {
                                insertResult.first->second = file = mSim->mRucio->CreateFile(outFile.second, now, SECONDS_PER_MONTH * 13);
                                std::shared_ptr<SReplica> srcReplica = mComputingStorageElement->CreateReplica(file, now);

                                assert(srcReplica);

                                srcReplica->Increase(outFile.second, now);
                                srcReplica->mExpiresAt = stageOutEndTime + 60;

                                InsertTmpReplica(srcReplica);

                                std::shared_ptr<SReplica> r = mDiskStorageElement->CreateReplica(file, now);
                                mTransferMgr->CreateTransfer(srcReplica, r, now, jobIt->mStageOutDuration);
                            }
                        }
                        jobIt = jobBatchIt->second.erase_after(prevIt);
                        continue;
                    }
                    else
                        mNextCallTick = std::min(mNextCallTick, jobEndTime);
                    ++prevIt;
                    ++jobIt;
                }
            }
            if (jobBatchIt->second.empty())
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

        for (const SJob& job : mCurJobBatch->second)
        {
            for (const std::pair<std::string, SpaceType>& inFile : job.mInputFiles)
            {
                std::shared_ptr<SReplica> srcReplica;
                std::shared_ptr<SFile> file;
                const std::size_t idx = std::clamp<std::size_t>(inFile.first.length(), LFN_MAP_IDX_OFFSET, NUM_LFN_MAPS);
                auto insertResult = mLFNToFile[idx].emplace(inFile.first, nullptr);
                if (insertResult.second == true)
                {
                    insertResult.first->second = file = mSim->mRucio->CreateFile(inFile.second, now, SECONDS_PER_MONTH * 13);
                    srcReplica = mDiskStorageElement->CreateReplica(file, now);

                    assert(srcReplica);

                    srcReplica->Increase(inFile.second, now);
                }
                else
                    file = insertResult.first->second;

                std::shared_ptr<SReplica> dstReplica = mComputingStorageElement->CreateReplica(file, now);
                if (!dstReplica)
                {
                    dstReplica = file->GetReplicaByStorageElement(mComputingStorageElement);
                    assert(dstReplica);
                    UpdateTmpReplicaExpiresAt(dstReplica, now + job.mStageInDuration + job.mJobDuration);
                }
                else
                {
                    dstReplica->mExpiresAt = now + job.mStageInDuration + job.mJobDuration + 60;
                    InsertTmpReplica(dstReplica);
                }

                if (!srcReplica)
                    srcReplica = file->GetReplicaByStorageElement(mDiskStorageElement);

                assert(srcReplica);

                mTransferMgr->CreateTransfer(srcReplica, dstReplica, now, job.mStageInDuration);
            }
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
        CScopedTimeDiff durationUpdate(nullptr, &mUpdateDurationSummed);

        if (mTmpReplicas.empty() && mJobBatchList.empty() && (mCurJobBatch == mJobBatchList.end()))
        {
            mNextCallTick = now + 20;
            if ((mTransferMgr->GetNumQueuedTransfers() + mTransferMgr->GetNumActiveTransfers()) == 0)
                mSim->Stop();
        }
        else
            mNextCallTick = std::numeric_limits<TickType>::max();

        assert(mDiskStorageElement && mComputingStorageElement);
        CleanupTmpReplicas(now);

        StageOut(now);

        StageIn(now);
    }

    void Shutdown(const TickType now)
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
};


bool CDeterministicSim01::SetupDefaults(const json& profileJson)
{
    if(!CDefaultBaseSim::SetupDefaults(profileJson))
        return false;

    std::shared_ptr<CDeterministicTransferGen> transferGen;
    std::shared_ptr<CFixedTimeTransferManager> manager;
    try
    {
        const json& transferGenCfg = profileJson.at("deterministicTransferGen");
        const std::string fileDataFilePathTmpl = transferGenCfg.at("fileDataFilePathTmpl").get<std::string>();
        const std::uint32_t fileDataFileFirstIdx = transferGenCfg.at("fileDataFileFirstIdx").get<std::uint32_t>();
        const std::string managerType = transferGenCfg.at("managerType").get<std::string>();
        const TickType managerTickFreq = transferGenCfg.at("managerTickFreq").get<TickType>();
        const TickType managerStartTick = transferGenCfg.at("managerStartTick").get<TickType>();
        if(managerType == "fixedTime")
        {
            manager = std::make_shared<CFixedTimeTransferManager>(managerTickFreq, managerStartTick);
            transferGen = std::make_shared<CDeterministicTransferGen>(this, manager, fileDataFilePathTmpl, fileDataFileFirstIdx);

            for(const json& storageElementName : transferGenCfg.at("diskStorageElements"))
            {
                CStorageElement* storageElement = mRucio->GetStorageElementByName(storageElementName.get<std::string>());
                if(storageElement)
                    transferGen->mDiskStorageElement = storageElement;
                else
                    std::cout<<"Failed to find diskStorageElements: "<<storageElementName.get<std::string>()<<std::endl;
            }

            for(const json& storageElementName : transferGenCfg.at("computingStorageElements"))
            {
                CStorageElement* storageElement = mRucio->GetStorageElementByName(storageElementName.get<std::string>());
                if(storageElement)
                    transferGen->mComputingStorageElement = storageElement;
                else
                    std::cout<<"Failed to find computingStorageElements: "<<storageElementName.get<std::string>()<<std::endl;
            }
        }
        else
        {
            std::cout << "Failed to load deterministic transfer gen cfg: only fixed transfer implemented" << std::endl;
            return false;
        }

        mRucio->mFileActionListeners.emplace_back(transferGen);
        mRucio->mReplicaActionListeners.emplace_back(transferGen);
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load deterministic transfer gen cfg: " << error.what() << std::endl;
        return false;
    }

    //std::shared_ptr<CReaperCaller> reaper;
    try
    {
        const json& reaperCfg = profileJson.at("reaper");
        const TickType tickFreq = reaperCfg.at("tickFreq").get<TickType>();
        const TickType startTick = reaperCfg.at("startTick").get<TickType>();
        //reaper = std::make_shared<CReaperCaller>(mRucio.get(), tickFreq, startTick);
    }
    catch (const json::out_of_range& error)
    {
        std::cout << "Failed to load reaper cfg: " << error.what() << std::endl;
        //reaper = std::make_shared<CReaperCaller>(mRucio.get(), 600, 600);
    }


    auto heartbeat = std::make_shared<CHeartbeat>(this, manager, nullptr, static_cast<std::uint32_t>(SECONDS_PER_DAY), static_cast<TickType>(SECONDS_PER_DAY));
    //heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["TransferUpdate"] = &(manager->mUpdateDurationSummed);
    heartbeat->mProccessDurations["TransferGen"] = &(transferGen->mUpdateDurationSummed);
    //heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);

    mSchedule.push(manager);
    mSchedule.push(transferGen);
    //mSchedule.push(reaper);
    mSchedule.push(heartbeat);

    return true;
}
