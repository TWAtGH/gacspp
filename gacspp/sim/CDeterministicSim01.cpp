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

    typedef std::pair<TickType, std::vector<SJob>> JobBatchType;

    std::list<JobBatchType> mJobBatchList;
    std::list<JobBatchType>::iterator mCurJobBatch;

    std::unordered_map<std::string, std::shared_ptr<SFile>> mLFNToFile;

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
            mLineDelim = mDataFile.widen('\n');
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
            SJob& newJob = mCurJobBatch->second.emplace_back();

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
                mDataFile.ignore(1);

                if(type == mTypeInputDelim)
                    newJob.mInputFiles.push_back(std::move(file));
                else
                    newJob.mOutputFiles.push_back(std::move(file));

                type = mDataFile.peek();
            }
        }

        mDataFile.ignore(2, mLineDelim);

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

    void OnUpdate(const TickType now)
    {
        if(!mDataFile.is_open() && mJobBatchList.empty() && (mTransferMgr->GetNumQueuedTransfers() + mTransferMgr->GetNumActiveTransfers()) == 0)
        {
            mSim->Stop();
            return;
        }

        assert(mDiskStorageElement && mComputingStorageElement);

        //STAGE-OUT
        mNextCallTick = now;
        for(auto it=mJobBatchList.begin(); it!=mJobBatchList.end(); ++it)
        {
            const TickType start = it->first;
            for(const SJob& job : mCurJobBatch->second)
            {
                const TickType durationSum = start + job.mStageInDuration + job.mJobDuration;
                if(now >= durationSum)
                {
                    for(const std::pair<std::string, SpaceType>& outFile : job.mOutputFiles)
                    {
                        std::shared_ptr<SFile> file;
                        auto insertResult = mLFNToFile.insert({outFile.first, file});

                        assert(insertResult.second);

                        insertResult.first->second = file = mSim->mRucio->CreateFile(outFile.second, now, SECONDS_PER_MONTH * 13);
                        std::shared_ptr<SReplica> srcReplica = mComputingStorageElement->CreateReplica(file, now);

                        assert(srcReplica);

                        srcReplica->Increase(outFile.second, now);

                        std::shared_ptr<SReplica> r = mDiskStorageElement->CreateReplica(file, now);
                        mTransferMgr->CreateTransfer(srcReplica, r, now, job.mStageOutDuration);
                    }
                }
                else if(mNextCallTick == now)
                    mNextCallTick = durationSum;
                else
                    mNextCallTick = std::min(mNextCallTick, durationSum);
            }
        }

        if(!mDataFile.is_open())
        {
            if(mNextCallTick == now)
                mNextCallTick = now + 20;
            return;
        }

        //STAGE-IN
        for(const SJob& job : mCurJobBatch->second)
        {
            for(const std::pair<std::string, SpaceType>& inFile : job.mInputFiles)
            {
                std::shared_ptr<SReplica> srcReplica;
                std::shared_ptr<SFile> file;
                auto insertResult = mLFNToFile.insert({inFile.first, file});
                if(insertResult.second == true)
                {
                    file = mSim->mRucio->CreateFile(inFile.second, now, SECONDS_PER_MONTH * 13);
                    insertResult.first->second = file;
                    srcReplica = mDiskStorageElement->CreateReplica(file, now);

                    assert(srcReplica);

                    srcReplica->Increase(inFile.second, now);
                }
                else
                    file = insertResult.first->second;

                if(!srcReplica)
                {
                    for(const std::shared_ptr<SReplica>& replica : file->mReplicas)
                    {
                        if(replica->GetStorageElement() == mDiskStorageElement)
                        {
                            srcReplica = replica;
                            break;
                        }
                    }
                }

                assert(srcReplica);
                std::shared_ptr<SReplica> r = mComputingStorageElement->CreateReplica(file, now);
                mTransferMgr->CreateTransfer(srcReplica, r, now, job.mStageInDuration);
            }
        }

        if(!ReadNextJobBatch() && (mNextCallTick == now))
            mNextCallTick = now + 20;
        else if(mNextCallTick == now)
            mNextCallTick = mCurJobBatch->first;
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

static CStorageElement* GetStorageElementByName(const std::vector<std::unique_ptr<CGridSite>>& gridSites, const std::string& name)
{
    for(const std::unique_ptr<CGridSite>& gridSite : gridSites)
        for(const std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
            if(storageElement->GetName() == name)
                return storageElement.get();
    return nullptr;
}

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
                CStorageElement* storageElement = GetStorageElementByName(mRucio->mGridSites, storageElementName.get<std::string>());
                if(storageElement)
                    transferGen->mDiskStorageElement = storageElement;
                else
                    std::cout<<"Failed to find diskStorageElements: "<<storageElementName.get<std::string>()<<std::endl;
            }

            for(const json& storageElementName : transferGenCfg.at("computingStorageElements"))
            {
                CStorageElement* storageElement = GetStorageElementByName(mRucio->mGridSites, storageElementName.get<std::string>());
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

    auto heartbeat = std::make_shared<CHeartbeat>(this, manager, nullptr, static_cast<std::uint32_t>(SECONDS_PER_DAY), static_cast<TickType>(SECONDS_PER_DAY));
    //heartbeat->mProccessDurations["DataGen"] = &(dataGen->mUpdateDurationSummed);
    heartbeat->mProccessDurations["TransferUpdate"] = &(manager->mUpdateDurationSummed);
    heartbeat->mProccessDurations["TransferGen"] = &(transferGen->mUpdateDurationSummed);
    //heartbeat->mProccessDurations["Reaper"] = &(reaper->mUpdateDurationSummed);

    mSchedule.push(manager);
    mSchedule.push(transferGen);
    mSchedule.push(heartbeat);

    return true;
}
