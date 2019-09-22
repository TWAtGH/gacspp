#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "IDatabase.hpp"



class COutput
{
private:
    COutput() = default;

    std::atomic_bool mIsConsumerRunning = false;
    std::thread mConsumerThread;

    std::atomic_size_t mConsumerIdx = 0;
    std::atomic_size_t mProducerIdx = 0;
    std::vector<std::unique_ptr<IInsertValuesContainer>> mInsertQueriesBuffer;

    std::shared_ptr<IDatabase> mDB;

public:
    std::vector<std::string> mInitQueries;
    std::vector<std::string> mShutdownQueries;

    COutput(const COutput&) = delete;
    COutput& operator=(const COutput&) = delete;
    COutput(const COutput&&) = delete;
    COutput& operator=(const COutput&&) = delete;

    ~COutput();

    static auto GetRef() -> COutput&;

    bool Initialise(const std::string& options, const std::size_t insertQueryBufferLen);
    bool StartConsumer();
    void Shutdown();

    auto CreatePreparedInsert(const std::string& queryTpl, const std::size_t numWildcards, char wildcard) -> std::shared_ptr<IPreparedInsert>;
    bool CreateTable(const std::string& tableName, const std::string& columns);
    bool CreateTable(const std::string& tableName, const std::vector<std::string>& columns);
    bool InsertRow(const std::string& tableName, const std::string& values);
    bool InsertRow(const std::string& tableName, const std::vector<std::string>& values);

    void QueueInserts(std::unique_ptr<IInsertValuesContainer>&& queriesContainer);

    void ConsumerThread();
};
