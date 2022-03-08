/**
 * @file   COutput.hpp
 * @brief  Contains the access point for using the output system
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 */

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "IDatabase.hpp"



 /**
 * @brief Singleton class providing functionality to control the output system
 * 
 * System will be initalised at program startup and opens a database connection in
 * accordance to the configuration. The system will then start a consumer thread that
 * consumes IInsertValuesContainer and inserts them into the database.
 */
class COutput
{
private:
    COutput() = default;

    /**
    * @brief atomic bool indicating whether the consumer thread is running
    */
    std::atomic_bool mIsConsumerRunning = false;

    /**
    * @brief handle of the consumer thread
    */
    std::thread mConsumerThread;

    /**
    * @brief current consumer index for the single producer single consumer queue
    */
    std::atomic_size_t mConsumerIdx = 0;

    /**
    * @brief current producer index for the single producer single consumer queue
    */
    std::atomic_size_t mProducerIdx = 0;

    /**
    * @brief Fixed size single producer single consumer queue
    */
    std::vector<std::unique_ptr<IInsertValuesContainer>> mInsertQueriesBuffer;

    /**
    * @brief shared pointer to the database connection
    */
    std::shared_ptr<IDatabase> mDB;

public:
    /**
    * @brief array of queries that will be executed on the database after the database was opened
    */
    std::vector<std::string> mInitQueries;

    /**
    * @brief array of queries that will be executed on the database before the database will be closed
    */
    std::vector<std::string> mShutdownQueries;

    COutput(const COutput&) = delete;
    COutput& operator=(const COutput&) = delete;
    COutput(const COutput&&) = delete;
    COutput& operator=(const COutput&&) = delete;

    ~COutput();

    /**
    * @brief Gets the singleton reference
    * 
    * @return reference to the singleton instance
    */
    static auto GetRef() -> COutput&;

    /**
    * @brief Initialises the output system, creating and opening a database
    * 
    * @param options options that decide which database is used and how it can be opened
    * @param insertQueryBufferLen size of the single producer single consusmer queue
    * 
    * @return true on success, false otherwise
    */
    bool Initialise(const std::string& options, const std::size_t insertQueryBufferLen);

    /**
    * @brief Starts the consumer thread
    * 
    * @return true on success, false otherwise
    */
    bool StartConsumer();

    /**
    * @brief Shutdown the output system. Waits for the consumer thread, executes the shutdown queries and closes the database.
    */
    void Shutdown();

    /**
    * @brief Convenience function to create a IPreparedInsert object. See IDataBase::CreatePreparedInsert()
    * 
    * @param queryTpl the query template
    * @param numWildcards number of wildcards in the template
    * @param wildcard character used to indicate wildcards
    * 
    * @return a shared pointer to an IPreparedInsert object on success, nullptr otherwise
    */
    auto CreatePreparedInsert(const std::string& queryTpl, const std::size_t numWildcards, char wildcard) -> std::shared_ptr<IPreparedInsert>;

    /**
    * @brief Convenience function to create a table at the database
    * 
    * @param tableName name of the table
    * @param columns column names of the table
    * 
    * @return true on success, false otherwise
    */
    bool CreateTable(const std::string& tableName, const std::string& columns);

    /**
    * @brief Convenience function to create a table at the database
    *
    * @param tableName name of the table
    * @param columns column names of the table
    *
    * @return true on success, false otherwise
    */
    bool CreateTable(const std::string& tableName, const std::vector<std::string>& columns);

    /**
    * @brief Convenience function to insert a row into a table
    *
    * @param tableName name of the table
    * @param values values to insert
    *
    * @return true on success, false otherwise
    */
    bool InsertRow(const std::string& tableName, const std::string& values);

    /**
    * @brief Convenience function to insert a row into a table
    *
    * @param tableName name of the table
    * @param values values to insert
    *
    * @return true on success, false otherwise
    */
    bool InsertRow(const std::string& tableName, const std::vector<std::string>& values);

    /**
    * @brief Adds an IInsertValuesContainer to the consumer queue
    *
    * @param queriesContainer unique pointer to the IInsertValuesContainer object (pointer will be consumed)
    *
    * @return true on success, false otherwise
    */
    void QueueInserts(std::unique_ptr<IInsertValuesContainer>&& queriesContainer);

    /**
    * @brief Method running the consumer thread logic
    */
    void ConsumerThread();
};
