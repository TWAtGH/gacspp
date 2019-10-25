#include <fstream>
#include <iomanip>
#include <iostream>
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

        return mDataFile.is_open();
    }

    struct SDataRow
    {
        std::uint64_t mStartTime;
        std::uint64_t mStageInDuration;
        std::uint64_t mJobDuration;
        std::uint64_t mStageOutDuration;
        std::uint64_t mPandaId;
        std::uint64_t mFileSize;
        std::string mLFN;
        std::string mType;
    };

    SDataRow mCurRow;

    bool ReadNextRow()
    {
        std::uint64_t tmpInt;
        char comma;
        std::string eol;

        while(mDataFile.is_open())
        {
            mDataFile >> mCurRow.mStartTime;
            mDataFile >> comma;

            mDataFile >> tmpInt;
            mDataFile >> comma;

            mDataFile >> mCurRow.mStageInDuration;
            mCurRow.mStageInDuration += tmpInt;
            mDataFile >> comma;

            mDataFile >> mCurRow.mJobDuration;
            mDataFile >> comma;

            mDataFile >> mCurRow.mStageOutDuration;
            mDataFile >> comma;

            mDataFile >> tmpInt;
            mCurRow.mStageOutDuration += tmpInt;
            mDataFile >> comma;

            mDataFile >> mCurRow.mPandaId;
            mDataFile >> comma;

            std::getline(mDataFile, mCurRow.mLFN, ',');
            std::getline(mDataFile, mCurRow.mType, ',');

            mDataFile >> mCurRow.mFileSize;
            std::getline(mDataFile, eol);

            if(mDataFile.eof())
                LoadNextFile();

            if(mCurRow.mType == "input" || mCurRow.mType == "output")
                return true;
        }
        return false;
    }

    std::shared_ptr<IPreparedInsert> mFileInsertQuery;
    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;

public:
    CStorageElement* mSrcStorageElement = nullptr;
    CStorageElement* mDstStorageElement = nullptr;

    CDeterministicTransferGen(IBaseSim* sim,
                              std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                              const std::string& filePathTmpl,
                              const std::uint32_t startFileIdx=0)
        : mSim(sim), mTransferMgr(transferMgr),
          mFilePathTmpl(filePathTmpl), mCurFileIdx(startFileIdx)
    {
        bool ok = LoadNextFile();
        assert(ok);
        ok = ReadNextRow();
        assert(ok);
    }

    void OnUpdate(const TickType now)
    {
        if(!mDataFile.is_open())
        {
            if((mTransferMgr->GetNumQueuedTransfers() + mTransferMgr->GetNumActiveTransfers()) == 0)
            {
                mSim->Stop();
                return;
            }
            mNextCallTick = now + 20;
        }

        assert(mSrcStorageElement && mDstStorageElement);

        while(mCurRow.mStartTime <= now)
        {
            std::shared_ptr<SReplica> srcReplica;
            std::shared_ptr<SFile> file;
            auto insertResult = mLFNToFile.insert({mCurRow.mLFN, file});
            if(insertResult.second == true)
            {
                file = mSim->mRucio->CreateFile(mCurRow.mFileSize, now, SECONDS_PER_MONTH * 13);
                insertResult.first->second = file;
                srcReplica = mSrcStorageElement->CreateReplica(file, now);

                assert(srcReplica);

                srcReplica->Increase(mCurRow.mFileSize, now);
            }
            else
                file = insertResult.first->second;

            if(!srcReplica)
            {
                for(const std::shared_ptr<SReplica>& replica : file->mReplicas)
                {
                    if(replica->GetStorageElement() == mSrcStorageElement)
                    {
                        srcReplica = replica;
                        break;
                    }
                }
            }

            assert(srcReplica);

            std::shared_ptr<SReplica> r = mDstStorageElement->CreateReplica(file, now);
            if(r)
            {
                mTransferMgr->CreateTransfer(srcReplica, r, now, mCurRow.mStageInDuration);
            }
            else
            {
                //r->mExpiresAt =
            }

            if(!ReadNextRow())
            {
                mNextCallTick = now + 20;
                return;
            }
        }
        mNextCallTick = mCurRow.mStartTime;
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

            for(const json& storageElementName : transferGenCfg.at("srcStorageElements"))
            {
                CStorageElement* storageElement = GetStorageElementByName(mRucio->mGridSites, storageElementName.get<std::string>());
                if(storageElement)
                    transferGen->mSrcStorageElement = storageElement;
                else
                    std::cout<<"Failed to find srcStorageElement: "<<storageElementName.get<std::string>()<<std::endl;
            }

            for(const json& storageElementName : transferGenCfg.at("dstStorageElements"))
            {
                CStorageElement* storageElement = GetStorageElementByName(mRucio->mGridSites, storageElementName.get<std::string>());
                if(storageElement)
                    transferGen->mDstStorageElement = storageElement;
                else
                    std::cout<<"Failed to find dstStorageElement: "<<storageElementName.get<std::string>()<<std::endl;
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
