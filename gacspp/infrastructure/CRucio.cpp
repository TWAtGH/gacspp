#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

#include "IActionListener.hpp"
#include "CRucio.hpp"
#include "CNetworkLink.hpp"
#include "CStorageElement.hpp"
#include "SFile.hpp"

#include "common/utils.hpp"

#include "third_party/nlohmann/json.hpp"

#define NUM_REPEAR_THREADS 1


/**
* @brief Interal class that can be implemented in various ways. However, the basic purpose is to remove all expired files and replicas.
*/
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

public:
    CReaper(CRucio* rucio);
    ~CReaper();

    /**
    * @brief method that can be executed in parallel by a thread to check certain parts of the file array for expired files
    * 
    * @param threadIdx the thread index
    */
    void ReaperWorker(const std::size_t threadIdx);

    /**
    * @brief Executes the reaper removing all expired files from the passed in array
    * 
    * @param files the array to check for expired files
    * @param now the current simulation time
    * 
    * @return the number of removed files
    */
    auto RunReaper(std::vector<std::unique_ptr<SFile>>& files, TickType now) -> std::size_t;
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
    (void)threadIdx;
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
        /*
        const float numElementsPerThread = mRucio->GetFiles().size() / static_cast<float>(NUM_REPEAR_THREADS);
        std::vector<std::unique_ptr<SFile>>::iterator fileIt = mFilesBeginIt + static_cast<std::size_t>(numElementsPerThread * threadIdx);
        const std::vector<std::unique_ptr<SFile>>::iterator endIt = mFilesBeginIt + static_cast<std::size_t>(numElementsPerThread * (threadIdx + 1));
        
        for(; fileIt != endIt; ++fileIt)
        {
            std::unique_ptr<SFile>& curFile = *fileIt;
            if(curFile->mExpiresAt <= mReaperWorkerNow)
            {
                curFile->Remove(mReaperWorkerNow);
                curFile = nullptr;
            }
            else
                curFile->RemoveExpiredReplicas(mReaperWorkerNow);
        }*/

        lastNow = mReaperWorkerNow;
        if((--mNumWorkingReapers) == 0)
            mFinishCondition.notify_one();
    }
}

auto CReaper::RunReaper(std::vector<std::unique_ptr<SFile>>& files, TickType now) -> std::size_t
{
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
        std::unique_ptr<SFile>& curFile = files[frontIdx];
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

    return numFiles - files.size();
}



CGridSite::~CGridSite() = default;

auto CGridSite::CreateStorageElement(std::string&& name, bool allowDuplicateReplicas, SpaceType limit) -> CStorageElement*
{
    mStorageElements.emplace_back(std::make_unique<CStorageElement>(std::move(name), this, allowDuplicateReplicas, limit));
    return mStorageElements.back().get();
}

auto CGridSite::GetStorageElements() const -> std::vector<CStorageElement*>
{
    std::vector<CStorageElement*> storageElements;
    for (const std::unique_ptr<CStorageElement>& storageElement : mStorageElements)
        storageElements.push_back(storageElement.get());
    return storageElements;
}



CRucio::CRucio()
{
    mReaper = std::make_unique<CReaper>(this);
}

CRucio::~CRucio() = default;

void CRucio::ReserveFileSpace(std::size_t amount)
{
    mFiles.reserve(mFiles.capacity() + amount);
}

auto CRucio::CreateFile(SpaceType size, TickType now, TickType lifetime) -> SFile*
{
    mFiles.emplace_back(std::make_unique<SFile>(size, now, lifetime, mFiles.size()));
    SFile* newFile = mFiles.back().get();

    for (IRucioActionListener* listener : mActionListener)
        listener->PostCreateFile(newFile, now);

    return newFile;
}

void CRucio::RemoveFile(SFile* file, TickType now)
{
    file->mExpiresAt = now;

    for (IRucioActionListener* listener : mActionListener)
        listener->PreRemoveFile(file, now);

    const std::size_t idxToDelete = file->mIndexAtRucio;
    
    assert(idxToDelete < mFiles.size());

    for (SReplica* replica : file->GetReplicas())
        replica->GetStorageElement()->RemoveReplica(replica, now, false);
    
    mFiles[idxToDelete] = std::move(mFiles.back());
    mFiles[idxToDelete]->mIndexAtRucio = idxToDelete;
    mFiles.pop_back();
}

void CRucio::RemoveAllFiles(TickType now)
{
    while (mFiles.size() > 0)
    {
        SFile* file = mFiles.back().get();
        
        file->mExpiresAt = now;

        for (IRucioActionListener* listener : mActionListener)
            listener->PreRemoveFile(file, now);

        for (SReplica* replica : file->GetReplicas())
            replica->GetStorageElement()->RemoveReplica(replica, now, false);

        mFiles.pop_back();
    }
}

auto CRucio::RemoveExpiredReplicasFromFile(SFile* file, TickType now) -> std::size_t
{
    std::vector<SReplica*> replicas = file->GetReplicas();
    for (SReplica* replica : replicas)
        if (replica->mExpiresAt <= now)
            replica->GetStorageElement()->RemoveReplica(replica, now);

    if (file->GetReplicas().empty())
    {
        RemoveFile(file, now);
        return replicas.size();
    }

    return replicas.size() - file->GetReplicas().size();
}

auto CRucio::ExtractExpiredReplicasFromFile(SFile* file, TickType now) -> std::vector<SReplica*>
{
    std::vector<SReplica*> expiredReplicas;

    for (SReplica* replica : file->GetReplicas())
        if (replica->mExpiresAt <= now)
            expiredReplicas.emplace_back(replica);

    return expiredReplicas;
}

auto CRucio::RunReaper(TickType now) -> std::size_t
{
    return mReaper->RunReaper(mFiles, now);
}

auto CRucio::CreateGridSite(std::string&& name, std::string&& locationName, std::uint8_t multiLocationIdx) -> CGridSite*
{
    mGridSites.emplace_back(std::make_unique<CGridSite>(std::move(name), std::move(locationName), multiLocationIdx));
    return mGridSites.back().get();
}

auto CRucio::GetStorageElementByName(const std::string& name) const -> CStorageElement*
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

                            const SpaceType limit = storageElementJson.contains("limit") ? storageElementJson["limit"].get<SpaceType>() : 0;
                            const bool duplicates = storageElementJson.contains("allowDuplicateReplicas") ? storageElementJson["allowDuplicateReplicas"].get<bool>() : false;
                            CStorageElement* se = site->CreateStorageElement(std::move(name), duplicates, limit);
                            if(se && storageElementJson.contains("accessLatency"))
                                se->mAccessLatency = IValueGenerator::CreateFromJson(storageElementJson.at("accessLatency"));
                            else
                               se->mAccessLatency = std::make_unique<CFixedValueGenerator>(0);
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
