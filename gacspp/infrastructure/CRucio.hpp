#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ISite.hpp"

#include "common/constants.h"
#include "common/IConfigConsumer.hpp"


struct SFile;

#define NUM_REPEAR_THREADS 16

class CGridSite : public ISite
{
public:
    using ISite::ISite;

	std::vector<std::unique_ptr<CStorageElement>> mStorageElements;

	CGridSite(std::string&& name, std::string&& locationName);
	CGridSite(CGridSite&&) = default;
	CGridSite& operator=(CGridSite&&) = default;

	CGridSite(CGridSite const&) = delete;
	CGridSite& operator=(CGridSite const&) = delete;

	auto CreateStorageElement(std::string&& name, const TickType accessLatency) -> CStorageElement*;
};

class CRucio : public IConfigConsumer
{
private:
    TickType mReaperWorkerNow = 0;
    std::atomic_bool mAreThreadsRunning = true;

    std::condition_variable mStartCondition;
    std::condition_variable mFinishCondition;
    std::mutex mConditionMutex;
    std::atomic_size_t mNumWorkingReapers = 0;

    std::unique_ptr<std::thread> mThreads[NUM_REPEAR_THREADS];

public:
    std::vector<std::shared_ptr<SFile>> mFiles;
    std::vector<std::unique_ptr<CGridSite>> mGridSites;

    std::vector<std::vector<std::weak_ptr<SFile>>*> mFileCreationListeners;

    CRucio();
    ~CRucio();

    auto CreateFile(const std::uint32_t size, const TickType expiresAt) -> std::shared_ptr<SFile>;
    auto CreateGridSite(std::string&& name, std::string&& locationName, const std::uint8_t multiLocationIdx) -> CGridSite*;
    auto RunReaper(const TickType now) -> std::size_t;
    void ReaperWorker(const std::size_t threadIdx);

    bool LoadConfig(const json& config) final;
};
