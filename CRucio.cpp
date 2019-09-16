#include <iostream>

#include "json.hpp"

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
							try
							{
								site->CreateStorageElement(storageElementJson.at("name").get<std::string>());
							}
							catch (const json::out_of_range& error)
							{
								std::cout << "Failed to add storage element: " << error.what() << std::endl;
								continue;
							}
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
