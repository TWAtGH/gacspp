#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

#include "CRucio.hpp"
#include "CNetworkLink.hpp"
#include "CStorageElement.hpp"
#include "SFile.hpp"

#include "third_party/json.hpp"

#define NUM_REPEAR_THREADS 1


class CReaper
{
private:
    CRucio* mRucio;

    TickType mReaperWorkerNow = 0;
    std::atomic_bool mAreThreadsRunning = true;

    std::condition_variable mStartCondition;
    std::condition_variable mFinishCondition;
    std::mutex mConditionMutex;
    std::atomic_size_t mNumWorkingReapers = 0;

    std::unique_ptr<std::thread> mThreads[NUM_REPEAR_THREADS];

    std::vector<std::vector<std::shared_ptr<SFile>>> mRemovedFilesPerThread{NUM_REPEAR_THREADS};
    std::vector<std::vector<std::shared_ptr<SReplica>>> mRemovedReplicasPerThread{NUM_REPEAR_THREADS};

public:
    CReaper(CRucio* rucio);
    ~CReaper();
    void ReaperWorker(const std::size_t threadIdx);
    auto RunReaper(const TickType now) -> std::size_t;
};

CReaper::CReaper(CRucio* rucio)
    : mRucio(rucio)
{
    for(std::size_t i = 0; i<NUM_REPEAR_THREADS; ++i)
        mThreads[i].reset(new std::thread(&CReaper::ReaperWorker, this, i));
}

CReaper::~CReaper()
{
    mAreThreadsRunning = false;
    mStartCondition.notify_all();
    for(std::unique_ptr<std::thread>& thread : mThreads)
        thread->join();
}

void CReaper::ReaperWorker(const std::size_t threadIdx)
{
    TickType lastNow = 0;
    auto waitFunc = [&]{return (lastNow < mReaperWorkerNow) || !mAreThreadsRunning;};
    while(mAreThreadsRunning)
    {
        {
            std::unique_lock<std::mutex> lock(mConditionMutex);
            mStartCondition.wait(lock, waitFunc);
        }

        if(!mAreThreadsRunning)
            return;

        std::vector<std::shared_ptr<SFile>>& files = mRucio->mFiles;
        std::vector<std::shared_ptr<SFile>>& removedFiles = mRemovedFilesPerThread[threadIdx];
        std::vector<std::shared_ptr<SReplica>>& removedReplicas = mRemovedReplicasPerThread[threadIdx];
        const float numElementsPerThread = files.size() / static_cast<float>(NUM_REPEAR_THREADS);
        const auto lastIdx = static_cast<std::size_t>(numElementsPerThread * (threadIdx + 1));
        for(std::size_t i = numElementsPerThread * threadIdx; i < lastIdx; ++i)
        {
            std::shared_ptr<SFile>& curFile = files[i];
            if(curFile->mExpiresAt <= mReaperWorkerNow)
            {
                if(!mRucio->mReplicaActionListeners.empty())
                    for(std::shared_ptr<SReplica>& replica : curFile->mReplicas)
                        removedReplicas.emplace_back(replica);

                curFile->Remove(mReaperWorkerNow);

                if(mRucio->mFileActionListeners.empty())
                    curFile = nullptr;
                else
                    removedFiles.emplace_back(std::move(curFile));
            }
            else if(mRucio->mReplicaActionListeners.empty())
                curFile->RemoveExpiredReplicas(mReaperWorkerNow);
            else
                curFile->ExtractExpiredReplicas(mReaperWorkerNow, removedReplicas);
        }

        lastNow = mReaperWorkerNow;
        if((--mNumWorkingReapers) == 0)
            mFinishCondition.notify_one();
    }
}

auto CReaper::RunReaper(const TickType now) -> std::size_t
{
    std::vector<std::shared_ptr<SFile>>& files = mRucio->mFiles;
    const std::size_t numFiles = files.size();
    if(numFiles < NUM_REPEAR_THREADS)
        return 0;

    assert(mReaperWorkerNow < now);

    {
        std::unique_lock<std::mutex> lock(mConditionMutex);
        mReaperWorkerNow = now;
        mNumWorkingReapers = NUM_REPEAR_THREADS;
        mStartCondition.notify_all();
        auto waitFunc = [this]{return mNumWorkingReapers == 0;};
        mFinishCondition.wait(lock, waitFunc);
    }

    std::size_t frontIdx = 0;
    std::size_t backIdx = numFiles - 1;

    while(backIdx > frontIdx && files[backIdx] == nullptr)
    {
        files.pop_back();
        --backIdx;
    }

    for(;frontIdx < backIdx; ++frontIdx)
    {
        std::shared_ptr<SFile>& curFile = files[frontIdx];
        if(curFile == nullptr)
        {
            std::swap(curFile, files[backIdx]);
            do
            {
                files.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && files[backIdx] == nullptr);
        }
    }

    if(backIdx == 0 && files.back() == nullptr)
        files.pop_back();

    std::vector<std::weak_ptr<IFileActionListener>>& fileListeners = mRucio->mFileActionListeners;
    std::vector<std::weak_ptr<IReplicaActionListener>> replicaListeners = mRucio->mReplicaActionListeners;
    if(!fileListeners.empty())
    {
        //get num to reserve mem
        std::size_t numRemoved = 0;
        for(std::vector<std::shared_ptr<SFile>>& removedFilesPerThread : mRemovedFilesPerThread)
            numRemoved += removedFilesPerThread.size();

        //put files in continous mem in form of weak_ptr
        std::vector<std::weak_ptr<SFile>> removedFiles;
        removedFiles.reserve(numRemoved);
        for(std::vector<std::shared_ptr<SFile>>& removedFilesPerThread : mRemovedFilesPerThread)
            for(std::shared_ptr<SFile>& file : removedFilesPerThread)
                removedFiles.emplace_back(file);

        //notify all listeners
        for(std::size_t i=0; i<fileListeners.size();)
        {
            std::shared_ptr<IFileActionListener> listener = fileListeners[i].lock();
            if(!listener)
            {
                //remove invalid listeners
                fileListeners[i] = std::move(fileListeners.back());
                fileListeners.pop_back();
                continue;
            }

            listener->OnFilesDeleted(now, removedFiles);

            ++i;
        }

        //clear refs to deleted files
        for(std::vector<std::shared_ptr<SFile>>& removedFilesPerThread : mRemovedFilesPerThread)
            removedFilesPerThread.clear();
    }


    if(!replicaListeners.empty())
    {
        //get num to reserve mem
        std::size_t numRemoved = 0;
        for(std::vector<std::shared_ptr<SReplica>>& removedReplicasPerThread : mRemovedReplicasPerThread)
            numRemoved += removedReplicasPerThread.size();

        //put files in continous mem in form of weak_ptr
        std::vector<std::weak_ptr<SReplica>> removedReplicas;
        removedReplicas.reserve(numRemoved);
        for(std::vector<std::shared_ptr<SReplica>>& removedReplicasPerThread : mRemovedReplicasPerThread)
            for(std::shared_ptr<SReplica>& replica : removedReplicasPerThread)
                removedReplicas.emplace_back(replica);

        //notify all listeners
        for(std::size_t i=0; i<replicaListeners.size();)
        {
            std::shared_ptr<IReplicaActionListener> listener = replicaListeners[i].lock();
            if(!listener)
            {
                //remove invalid listeners
                replicaListeners[i] = std::move(replicaListeners.back());
                replicaListeners.pop_back();
                continue;
            }

            listener->OnReplicasDeleted(now, removedReplicas);

            ++i;
        }

        //clear refs to deleted files
        for(std::vector<std::shared_ptr<SReplica>>& removedReplicasPerThread : mRemovedReplicasPerThread)
            removedReplicasPerThread.clear();
    }

    return numFiles - files.size();
}



CGridSite::~CGridSite() = default;

auto CGridSite::CreateStorageElement(std::string&& name, bool forbidDuplicatedReplicas) -> CStorageElement*
{
    mStorageElements.emplace_back(std::make_unique<CStorageElement>(std::move(name), this, forbidDuplicatedReplicas));
    return mStorageElements.back().get();
}

void CGridSite::GetStorageElements(std::vector<CStorageElement*>& storageElements)
{
    storageElements.reserve(mStorageElements.size());
    for (std::unique_ptr<CStorageElement>& storageElement : mStorageElements)
        storageElements.push_back(storageElement.get());
}



CRucio::CRucio()
{
    mReaper = std::make_unique<CReaper>(this);
}

CRucio::~CRucio() = default;

auto CRucio::CreateFile(const SpaceType size, const TickType now, const TickType lifetime) -> std::shared_ptr<SFile>
{
    std::shared_ptr<SFile> newFile = std::make_shared<SFile>(size, now, lifetime);
    mFiles.emplace_back(newFile);

    //notify all listeners
    for(std::size_t i=0; i<mFileActionListeners.size();)
    {
        std::shared_ptr<IFileActionListener> listener = mFileActionListeners[i].lock();
        if(!listener)
        {
            //remove invalid listeners
            mFileActionListeners[i] = std::move(mFileActionListeners.back());
            mFileActionListeners.pop_back();
            continue;
        }

        listener->OnFileCreated(now, newFile);

        ++i;
    }

    return newFile;
}

auto CRucio::CreateGridSite(std::string&& name, std::string&& locationName, const std::uint8_t multiLocationIdx) -> CGridSite*
{
    CGridSite* newSite = new CGridSite(std::move(name), std::move(locationName), multiLocationIdx);
    mGridSites.emplace_back(newSite);
    return newSite;
}

auto CRucio::RunReaper(const TickType now) -> std::size_t
{
    return mReaper->RunReaper(now);
}

auto CRucio::GetStorageElementByName(const std::string& name) -> CStorageElement*
{
    for (const std::unique_ptr<CGridSite>& gridSite : mGridSites)
        for (const std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
            if (storageElement->GetName() == name)
                return storageElement.get();
    return nullptr;
}

bool CRucio::LoadConfig(const json& config)
{
    if (!config.contains("rucio"))
        return false;

    const json& rucioCfgJson = config["rucio"];

    try
    {
        for (const json& siteJson : rucioCfgJson.at("sites"))
        {
            std::unordered_map<std::string, std::string> customConfig;
            CGridSite* site = nullptr;
            try
            {
                site = CreateGridSite(siteJson.at("name").get<std::string>(),
                    siteJson.at("location").get<std::string>(),
                    siteJson.at("multiLocationIdx").get<std::uint8_t>());

                for (const auto& [siteJsonKey, siteJsonValue] : siteJson.items())
                {
                    if (siteJsonKey == "storageElements")
                    {
                        assert(siteJsonValue.is_array());
                        for (const json& storageElementJson : siteJsonValue)
                        {
                            std::string name;
                            try
                            {
                                storageElementJson.at("name").get_to(name);
                            }
                            catch (const json::out_of_range& error)
                            {
                                std::cout << "Failed getting settings for storage element: " << error.what() << std::endl;
                                continue;
                            }

                            if(storageElementJson.contains("forbidDuplicatedReplicas"))
                                site->CreateStorageElement(std::move(name), storageElementJson.at("forbidDuplicatedReplicas").get<bool>());
                            else
                                site->CreateStorageElement(std::move(name));
                        }
                    }
                    else if ((siteJsonKey == "name") || (siteJsonKey == "location") || (siteJsonKey == "multiLocationIdx"))
                        continue;
                    else if (siteJsonValue.type() == json::value_t::string)
                        customConfig[siteJsonKey] = siteJsonValue.get<std::string>();
                    else
                        customConfig[siteJsonKey] = siteJsonValue.dump();
                }
                site->mCustomConfig = std::move(customConfig);
            }
            catch (const json::out_of_range& error)
            {
                std::cout << "Failed to add site: " << error.what() << std::endl;
                continue;
            }
        }
    }
    catch (const json::exception& error)
    {
        std::cout << "Failed to load sites: " << error.what() << std::endl;
        return false;
    }

    return true;
}
