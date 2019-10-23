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


class CDeterministicTransferGen : public CScheduleable
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

        mDataFile.open(filePathBuilder.str());
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

        if(!mDataFile)
            LoadNextFile();

        while(mDataFile && !mDataFile.eof())
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

            if(mCurRow.mType == "input" || mCurRow.mType == "output")
                return true;

            if(mDataFile.eof())
                LoadNextFile();
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
        bool ok = ReadNextRow();
        assert(ok);
        mFileInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Files(id, createdAt, expiredAt, filesize) FROM STDIN with(FORMAT csv);", 4, '?');
        mReplicaInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Replicas(id, fileId, storageElementId, createdAt, expiredAt) FROM STDIN with(FORMAT csv);", 5, '?');
    }

    void OnUpdate(const TickType now)
    {
        assert(mSrcStorageElement && mDstStorageElement);

        while(mCurRow.mStartTime <= now)
        {
            /*
            std::uint64_t mStartTime;
            std::uint64_t mStageInDuration;
            std::uint64_t mJobDuration;
            std::uint64_t mStageOutDuration;
            std::uint64_t mPandaId;
            std::uint64_t mFileSize;
            std::string mLFN;
            std::string mType;
            */

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
            assert(r);

            mTransferMgr->CreateTransfer(srcReplica, r, now, mCurRow.mStageInDuration);

            if(ReadNextRow())
                mNextCallTick = mCurRow.mStartTime;
            else
                mNextCallTick = 0;
        }
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
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load deterministic transfer gen cfg: " << error.what() << std::endl;
        return false;
    }

    mSchedule.push(manager);
    mSchedule.push(transferGen);

    return true;
}
