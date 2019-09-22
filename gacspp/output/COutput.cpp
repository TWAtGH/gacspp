#include <cassert>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "COutput.hpp"
#include "CDatabasePSQL.hpp"

#include "common/constants.h"


auto COutput::GetRef() -> COutput&
{
    static COutput instance;
    return instance;
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
	{
		std::cout << "Waiting for last inserts..." << std::endl;
		mConsumerThread.join();
	}

    if(mDB)
    {
		if(!mShutdownQueries.empty())
		{
			std::cout << "Executing post sim queries..." << std::endl;
			mDB->BeginTransaction();
			for(const std::string& query : mShutdownQueries)
				mDB->ExecuteQuery(query);
			mDB->EndTransaction();
			mShutdownQueries.clear();
		}

        mDB->Close();
        mDB = nullptr;
    }
}

auto COutput::CreatePreparedInsert(const std::string& queryTpl, const std::size_t numWildcards, char wildcard) -> std::shared_ptr<IPreparedInsert>
{
    assert(mDB != nullptr);
    return mDB->PrepareInsert(mDB, queryTpl, numWildcards, wildcard);
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
    if(queriesContainer->IsEmpty())
        return;

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
    constexpr std::size_t mergeLimit = 4096;
    mDB->BeginTransaction();

    const std::size_t bufLen = mInsertQueriesBuffer.size();
    while(mIsConsumerRunning || (mConsumerIdx != mProducerIdx))
    {
        std::vector<std::unique_ptr<IInsertValuesContainer>> mergedContainers;
        std::size_t numMerged = 0;
        while((mConsumerIdx != mProducerIdx) && (numMerged<mergeLimit))
        {
            std::unique_ptr<IInsertValuesContainer> curContainer = std::move(mInsertQueriesBuffer[mConsumerIdx]);
            mConsumerIdx = (mConsumerIdx + 1) % bufLen;

            if(curContainer->IsMergingSupported())
            {
                bool wasMerged = false;
                for(std::size_t i=0; i<mergedContainers.size() && !wasMerged; ++i)
                    wasMerged = mergedContainers[i]->MergeIfPossible(curContainer);
                if(!wasMerged)
                    mergedContainers.emplace_back(std::move(curContainer));
                ++numMerged;
            }
            else
                curContainer->InsertValues();
        }

        for(std::unique_ptr<IInsertValuesContainer>& curContainer : mergedContainers)
            curContainer->InsertValues();

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    mDB->EndTransaction();
}
