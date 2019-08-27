#include <cassert>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>

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

bool COutput::Initialise(const std::string& params)
{
    assert(mDB == nullptr);
    mDB = std::make_shared<psql::CDatabase>();

    return mDB->Open(params);
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

    mDB->Close();
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

    std::size_t newProducerIdx = (mProducerIdx + 1) % OUTPUT_BUF_SIZE;
    while(newProducerIdx == mConsumerIdx)
    {
        assert(mIsConsumerRunning);
        newProducerIdx = (mProducerIdx + 1) % OUTPUT_BUF_SIZE;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    mInsertQueriesBuffer[mProducerIdx] = std::move(queriesContainer);
    mProducerIdx = newProducerIdx;
}

void COutput::ConsumerThread()
{
    mDB->BeginTransaction();

    std::size_t numInsertedCurTransaction = 0;
    while(mIsConsumerRunning || (mConsumerIdx != mProducerIdx))
    {
        while(mConsumerIdx != mProducerIdx)
        {
            if(numInsertedCurTransaction > 25000)
            {
                mDB->CommitAndBeginTransaction();
                numInsertedCurTransaction = 0;
            }

            numInsertedCurTransaction += mInsertQueriesBuffer[mConsumerIdx]->InsertValues();

            mInsertQueriesBuffer[mConsumerIdx] = nullptr;
            mConsumerIdx = (mConsumerIdx + 1) % OUTPUT_BUF_SIZE;
        }

        // try to use time while buf is empty by commiting the transactionn
        if(numInsertedCurTransaction > 1000)
        {
            mDB->CommitAndBeginTransaction();
            numInsertedCurTransaction = 0;
        }
        else
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    mDB->EndTransaction();
}
