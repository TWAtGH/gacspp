#include <cassert>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>

#include <iostream>

#include "constants.h"
#include "COutput.hpp"
#include "CDatabasePSQL.hpp"


auto COutput::GetRef() -> COutput&
{
    static COutput mInstance;
    return mInstance;
}

COutput::~COutput()
{
    Shutdown();
}

bool COutput::Initialise(const std::string& params, const std::size_t insertQueryBufferLen)
{
    assert(mDB == nullptr);
    mDB = std::make_shared<psql::CDatabase>();

    if(mDB->Open(params))
    {
        if(!mDB->BeginTransaction())
            return false;

        for(const std::string& query : mInitQueries)
        {
            if(!mDB->ExecuteQuery(query))
                return false;
        }

        if(!mDB->EndTransaction())
            return false;

        mInitQueries.clear();
        mInsertQueriesBuffer.resize(insertQueryBufferLen);

        return true;
    }
    return false;
}

bool COutput::StartConsumer()
{
    if(mConsumerThread.joinable())
        return false;

    mIsConsumerRunning = true;
    mConsumerThread = std::thread(&COutput::ConsumerThread, this);

    return true;
}

void COutput::Shutdown()
{
    mIsConsumerRunning = false;
    if(mConsumerThread.joinable())
        mConsumerThread.join();

    if(mDB)
    {
        mDB->BeginTransaction();
        for(const std::string& query : mShutdownQueries)
            mDB->ExecuteQuery(query);
        mDB->EndTransaction();
        mShutdownQueries.clear();

        mDB->Close();
        mDB = nullptr;
    }
}

auto COutput::CreatePreparedInsert(const std::string& queryTpl, char wildcard) -> std::shared_ptr<IPreparedInsert>
{
    assert(mDB != nullptr);
    return mDB->PrepareInsert(mDB, queryTpl, wildcard);

}


bool COutput::CreateTable(const std::string& tableName, const std::string& columns)
{
    if(mIsConsumerRunning)
        return false;
    const std::string str = "CREATE TABLE " + tableName + "(" + columns + ");";
    return mDB->ExecuteQuery(str);
}
bool COutput::CreateTable(const std::string& tableName, const std::vector<std::string>& columns)
{
    std::stringstream ss;
    ss << columns[0];
    for(std::size_t i=1; i<columns.size(); ++i)
        ss << ',' << columns[i];
    return CreateTable(tableName, ss.str());
}

bool COutput::InsertRow(const std::string& tableName, const std::string& row)
{
    if(mIsConsumerRunning)
        return false;
    const std::string str = "INSERT INTO " + tableName + " VALUES (" + row + ");";
    return mDB->ExecuteQuery(str);
}
bool COutput::InsertRow(const std::string& tableName, const std::vector<std::string>& values)
{
    std::stringstream ss;
    ss << values[0];
    for(std::size_t i=1; i<values.size(); ++i)
        ss << ',' << values[i];
    return InsertRow(tableName, ss.str());
}

void COutput::QueueInserts(std::unique_ptr<IInsertValuesContainer>&& queriesContainer)
{
    assert(queriesContainer != nullptr);
    const std::size_t bufLen = mInsertQueriesBuffer.size();

    std::size_t newProducerIdx = (mProducerIdx + 1) % bufLen;
    while(newProducerIdx == mConsumerIdx)
    {
        assert(mIsConsumerRunning);
        newProducerIdx = (mProducerIdx + 1) % bufLen;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    mInsertQueriesBuffer[mProducerIdx] = std::move(queriesContainer);
    mProducerIdx = newProducerIdx;
}

void COutput::ConsumerThread()
{
    mDB->BeginTransaction();

    const std::size_t bufLen = mInsertQueriesBuffer.size();
    while(mIsConsumerRunning || (mConsumerIdx != mProducerIdx))
    {
        while(mConsumerIdx != mProducerIdx)
        {
            mInsertQueriesBuffer[mConsumerIdx]->InsertValues();

            mInsertQueriesBuffer[mConsumerIdx] = nullptr;
            mConsumerIdx = (mConsumerIdx + 1) % bufLen;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    mDB->EndTransaction();
}
