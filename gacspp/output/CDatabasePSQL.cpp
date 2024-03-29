#include <cassert>
#include <iostream>
#include <sstream>

#include "CDatabasePSQL.hpp"

namespace psql
{

#ifdef WITH_PSQL
CResult::CResult(PGresult* result)
    : mResult(result)
{}

CResult::~CResult()
{
    PQclear(mResult);
    mResult = nullptr;
}

CResult::CResult(CResult&& other)
    : mResult(other.mResult)
{
    other.mResult = nullptr;
}

CResult& CResult::operator=(CResult&& other)
{
    mResult = other.mResult;
    other.mResult = nullptr;
    return *this;
}
#endif

CResult::operator bool() const
{

#ifdef WITH_PSQL
    if(mResult)
    {
        const auto status = PQresultStatus(mResult);
        if(status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK || status == PGRES_COPY_IN)
            return true;
    }
#endif
    return false;
}

std::string CResult::str() const
{
#ifdef WITH_PSQL
    return std::string(PQresultErrorMessage(mResult));
#else
    return "";
#endif
}


CInsertValuesContainer::CInsertValuesContainer(const std::shared_ptr<IDatabase>& db, const std::string& id, std::size_t numParameters, std::size_t numReserveValues)
    : IInsertValuesContainer(db),
      mID(id),
      mNumParameters(numParameters)
{
    (void)numReserveValues;
}

void CInsertValuesContainer::AddValue(double value)
{
    mValues += std::to_string(value) + std::string(",");
}

void CInsertValuesContainer::AddValue(int value)
{
    mValues += std::to_string(value) + std::string(",");
}

void CInsertValuesContainer::AddValue(std::uint32_t value)
{
    mValues += std::to_string(value) + std::string(",");
}

void CInsertValuesContainer::AddValue(std::uint64_t value)
{
    mValues += std::to_string(value) + std::string(",");
}

void CInsertValuesContainer::AddValue(const std::string& value)
{
    mValues.reserve(mValues.size() + value.size() + 7);
    mValues += '"';
    for (auto c : value)
    {
        if (c == '"')
            mValues += '"';
        mValues += c;
    }
    mValues += "\",";
}

void CInsertValuesContainer::AddValue(std::string&& value)
{
    AddValue(value);
}

bool CInsertValuesContainer::MergeIfPossible(std::unique_ptr<IInsertValuesContainer>& other)
{
    CInsertValuesContainer* otherCasted = dynamic_cast<CInsertValuesContainer*>(other.get());
    if(!otherCasted)
        return false;
    if(mID != otherCasted->mID)
        return false;
    if(!otherCasted->mValues.empty())
        mValues += std::move(otherCasted->mValues);
    other.reset(nullptr);
    return true;
}

auto CInsertValuesContainer::InsertValues() -> std::size_t
{
#ifdef WITH_PSQL
    PGconn* dbConnection = nullptr;
    {
        CDatabase* dbPSQL = dynamic_cast<CDatabase*>(mDB.get());
        assert(dbPSQL != nullptr);
        dbConnection = dbPSQL->GetConnection();
        assert(dbConnection != nullptr);
    }

    if(mValues.empty() && (mNumParameters == 0))
    {
        CResult res(PQexecPrepared(dbConnection, mID.c_str(), 0, nullptr, nullptr, nullptr, 0));
        if(!res)
            std::cout<<"Insertion of row failed:"<<std::endl<<res.str()<<std::endl;
        return 1;
    }
    else if(mValues.empty())
        return 0;

    assert(mNumParameters > 0);

    CResult res(PQexecPrepared(dbConnection, mID.c_str(), 0, nullptr, nullptr, nullptr, 0));
    if(!res)
    {
        std::cout<<"Bulk insertion failed:"<<std::endl<<res.str()<<std::endl;
        return 0;
    }

    std::size_t curDelimCnt = 0;
    for(auto& c : mValues)
    {
        if(c == ',')
        {
            curDelimCnt = (curDelimCnt + 1) % mNumParameters;
            if(curDelimCnt == 0)
                c = '\n';
        }
    }

    int resQueue = PQputCopyData(dbConnection, mValues.c_str(), mValues.length());
    int resEnd = PQputCopyEnd(dbConnection, nullptr);
    if(resQueue != 1)
        std::cout<<"Queing data failed: "<<resQueue<<std::endl;
    if(resEnd == 1)
    {
        res = CResult(PQgetResult(dbConnection));
        if(!res)
        {
            std::cout<<"Getting results after copy end failed:"<<std::endl<<res.str()<<std::endl;
            return 0;
        }
    }
    else
        std::cout<<"Copy end failed: "<<resEnd<<std::endl;

    return (mValues.size() / mNumParameters);
#else
    return 0;
#endif
}


CPreparedInsert::CPreparedInsert(const std::shared_ptr<IDatabase>& db, std::string&& id, std::size_t numParameters)
    : IPreparedInsert(db, numParameters),
      mID(id)
{}

auto CPreparedInsert::CreateValuesContainer(std::size_t numReserveValues) -> std::unique_ptr<IInsertValuesContainer>
{
    return std::make_unique<CInsertValuesContainer>(mDB, mID, mNumParameters, numReserveValues);
}


CDatabase::~CDatabase()
{
    Close();
}

bool CDatabase::Open(const std::string& params)
{
#ifdef WITH_PSQL
    mConnection = PQconnectdb(params.c_str());
    if(mConnection == nullptr)
        return false;
    return (PQstatus(mConnection) != CONNECTION_BAD);
#else
    return false;
#endif
}

bool CDatabase::Close()
{
#ifdef WITH_PSQL
    if(mConnection)
    {
        PQfinish(mConnection);
        mConnection = nullptr;
    }
#endif
    return true;
}


bool CDatabase::ExecuteQuery(const std::string& query)
{
#ifdef WITH_PSQL
    CResult res(PQexec(mConnection, query.c_str()));
    if(!res)
        std::cout<<"Query failed:"<<std::endl<<query<<std::endl<<res.str()<<std::endl;
    return (res == true);
#else
    return false;
#endif
}

auto CDatabase::PrepareInsert(const std::shared_ptr<IDatabase>& db, const std::string& queryTpl, std::size_t numWildcards, char wildcard) -> std::shared_ptr<IPreparedInsert>
{
    std::string id = std::to_string(++mNumPreparerdQueries);

    std::size_t numParameters = 0;
    std::stringstream queryTplStream;
    for(auto c : queryTpl)
    {
        if (c == wildcard)
        {
            ++numParameters;
            queryTplStream << "$" << numParameters;
        }
        else
            queryTplStream << c;
    }

    std::shared_ptr<CPreparedInsert> preparedInsert;
#ifdef WITH_PSQL
    CResult res(PQprepare(mConnection, id.c_str(), queryTplStream.str().c_str(), numWildcards, nullptr));
    if(res)
        preparedInsert = std::make_shared<CPreparedInsert>(db, std::move(id), numWildcards);
    else
        std::cout<<"Preparing query failed:"<<std::endl<<queryTplStream.str()<<std::endl<<res.str()<<std::endl;
#endif
    return preparedInsert;
}

bool CDatabase::BeginTransaction()
{
    return ExecuteQuery("BEGIN");
}

bool CDatabase::CommitAndBeginTransaction()
{
    if(!ExecuteQuery("COMMIT"))
        return false;
    return ExecuteQuery("BEGIN");
}

bool CDatabase::EndTransaction()
{
    return ExecuteQuery("COMMIT");
}

}
