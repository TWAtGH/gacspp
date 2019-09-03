#include <iostream>

#include "json.hpp"

#include "CLinkSelector.hpp"
#include "CRucio.hpp"
#include "CStorageElement.hpp"
#include "SFile.hpp"



auto CGridSite::CreateStorageElement(std::string&& name) -> CStorageElement*
{
    CStorageElement* newStorageElement = new CStorageElement(std::move(name), this);
    mStorageElements.emplace_back(newStorageElement);
    return newStorageElement;
}



CRucio::CRucio()
{
    mFiles.reserve(150000);
    for(std::size_t i = 0; i<NUM_REPEAR_THREADS; ++i)
        mThreads[i].reset(new std::thread(&CRucio::ReaperWorker, this, i));
}

CRucio::~CRucio()
{
    mAreThreadsRunning = false;
    mStartCondition.notify_all();
    for(std::size_t i = 0; i<NUM_REPEAR_THREADS; ++i)
        mThreads[i]->join();
}

auto CRucio::CreateFile(const std::uint32_t size, const TickType expiresAt) -> SFile*
{
    SFile* newFile = new SFile(size, expiresAt);
    mFiles.emplace_back(newFile);
    return newFile;
}
auto CRucio::CreateGridSite(std::string&& name, std::string&& locationName, const std::uint8_t multiLocationIdx) -> CGridSite*
{
    CGridSite* newSite = new CGridSite(std::move(name), std::move(locationName), multiLocationIdx);
    mGridSites.emplace_back(newSite);
    return newSite;
}

void printTS(std::size_t i, const std::string& n)
{
    static std::mutex coutMutex;
    std::unique_lock<std::mutex> lock(coutMutex);
    std::cout<<i<<" "<<n<<std::endl;
}

void CRucio::ReaperWorker(const std::size_t threadIdx)
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

        const float numElementsPerThread = mFiles.size() / static_cast<float>(NUM_REPEAR_THREADS);
        const auto lastIdx = static_cast<std::size_t>(numElementsPerThread * (threadIdx + 1));
        for(std::size_t i = numElementsPerThread * threadIdx; i < lastIdx; ++i)
        {
            std::unique_ptr<SFile>& curFile = mFiles[i];
            if(curFile->mExpiresAt <= mReaperWorkerNow)
            {
                curFile->Remove(mReaperWorkerNow);
                curFile.reset(nullptr);
            }
            else
                curFile->RemoveExpiredReplicas(mReaperWorkerNow);
        }

        lastNow = mReaperWorkerNow;
        if((--mNumWorkingReapers) == 0)
            mFinishCondition.notify_one();
    }
}

auto CRucio::RunReaper(const TickType now) -> std::size_t
{
    const std::size_t numFiles = mFiles.size();
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

    while(backIdx > frontIdx && mFiles[backIdx] == nullptr)
    {
        mFiles.pop_back();
        --backIdx;
    }

    for(;frontIdx < backIdx; ++frontIdx)
    {
        std::unique_ptr<SFile>& curFile = mFiles[frontIdx];
        if(curFile == nullptr)
        {
            std::swap(curFile, mFiles[backIdx]);
            do
            {
                mFiles.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && mFiles[backIdx] == nullptr);
        }
    }

    if(backIdx == 0 && mFiles.back() == nullptr)
        mFiles.pop_back();

    return numFiles - mFiles.size();
}

bool CRucio::TryConsumeConfig(const json& json)
{
    json::const_iterator rootIt = json.find("rucio");
    if( rootIt == json.cend() )
        return false;

    for( const auto& [key, value] : rootIt.value().items() )
    {
        if( key == "sites" )
        {
            for(const auto& siteJson : value)
            {
                std::unique_ptr<std::uint8_t> multiLocationIdx;
                std::string siteName, siteLocation;
                nlohmann::json storageElementsJson;
                std::unordered_map<std::string, std::string> customConfig;
                for(const auto& [siteJsonKey, siteJsonValue] : siteJson.items())
                {
                    if(siteJsonKey == "name")
                        siteName = siteJsonValue.get<std::string>();
                    else if(siteJsonKey == "location")
                        siteLocation = siteJsonValue.get<std::string>();
                    else if(siteJsonKey == "multiLocationIdx")
                        multiLocationIdx = std::make_unique<std::uint8_t>(siteJsonValue.get<std::uint8_t>());
                    else if(siteJsonKey == "storageElements")
                        storageElementsJson = siteJsonValue;
                    else if(siteJsonValue.type() == json::value_t::string)
                        customConfig[siteJsonKey] = siteJsonValue.get<std::string>();
                    else
                        customConfig[siteJsonKey] = siteJsonValue.dump();
                }

                if (multiLocationIdx == nullptr)
                {
                    std::cout << "Couldn't find multiLocationIdx attribute of site" << std::endl;
                    continue;
                }

                if (siteName.empty())
                {
                    std::cout << "Couldn't find name attribute of site" << std::endl;
                    continue;
                }

                if (siteLocation.empty())
                {
                    std::cout << "Couldn't find location attribute of site: " << siteName << std::endl;
                    continue;
                }

                std::cout << "Adding site " << siteName << " in " << siteLocation << std::endl;
                CGridSite *site = CreateGridSite(std::move(siteName), std::move(siteLocation), *multiLocationIdx);
                site->mCustomConfig = std::move(customConfig);

                if (storageElementsJson.empty())
                {
                    std::cout << "No storage elements to create for this site" << std::endl;
                    continue;
                }

                for(const auto& storageElementJson : storageElementsJson)
                {
                    std::string storageElementName;
                    for(const auto& [storageElementJsonKey, storageElementJsonValue] : storageElementJson.items())
                    {
                        if(storageElementJsonKey == "name")
                            storageElementName = storageElementJsonValue.get<std::string>();
                        else
                            std::cout << "Ignoring unknown attribute while loading StorageElements: " << storageElementJsonKey << std::endl;
                    }

                    if (storageElementName.empty())
                    {
                        std::cout << "Couldn't find name attribute of StorageElement" << std::endl;
                        continue;
                    }

                    std::cout << "Adding StorageElement " << storageElementName << std::endl;
                    site->CreateStorageElement(std::move(storageElementName));
                }
            }
        }
    }
    return true;
}
