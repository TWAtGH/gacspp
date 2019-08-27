#include <cassert>
#include <sstream>

#include "CDatabasePSQL.hpp"

namespace psql
{

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

CResult::operator bool() const
{
    if(mResult)
    {
        const auto status = PQresultStatus(mResult);
        if(status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK)
            return true;
    }
    return false;
}


CInsertValuesContainer::CInsertValuesContainer(const std::shared_ptr<IDatabase>& db, const std::string& id, std::size_t numParameters, std::size_t numReserveValues)
    : IInsertValuesContainer(db),
      mID(id),
      mNumParameters(numParameters)
{
    if(numReserveValues > 0)
        mValues.reserve(numReserveValues);
}

void CInsertValuesContainer::AddValue(double value)
{
    mValues.emplace_back(std::move(std::to_string(value)));
}

void CInsertValuesContainer::AddValue(int value)
{
    mValues.emplace_back(std::move(std::to_string(value)));
}

void CInsertValuesContainer::AddValue(std::uint32_t value)
{
    mValues.emplace_back(std::move(std::to_string(value)));
}

void CInsertValuesContainer::AddValue(std::uint64_t value)
{
    mValues.emplace_back(std::move(std::to_string(value)));
}

void CInsertValuesContainer::AddValue(const std::string& value)
{
    mValues.emplace_back(value);
}

void CInsertValuesContainer::AddValue(std::string&& value)
{
    mValues.emplace_back(value);
}


auto CInsertValuesContainer::InsertValues() -> std::size_t
{
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
        return 1;
    }

    assert(mNumParameters > 0);
    assert((mValues.size() % mNumParameters) == 0);

    std::vector<const char*> paramValues(mNumParameters);
    for(std::size_t i=0; i<mValues.size(); ++i)
    {
        for(std::size_t j=0; j<mNumParameters; ++j)
            paramValues.push_back(mValues[i+j].c_str());
        CResult res(PQexecPrepared(dbConnection, mID.c_str(), mNumParameters, paramValues.data(), nullptr, nullptr, 0));
    }

    return (mValues.size() / mNumParameters);
}


CPreparedInsert::CPreparedInsert(const std::shared_ptr<IDatabase>& db, std::string&& id, std::size_t numParameters)
    : IPreparedInsert(db, numParameters),
      mID(id)
{}

auto CPreparedInsert::CreateValuesContainer(std::size_t numReserveValues) -> std::unique_ptr<IInsertValuesContainer>
{
    return std::make_unique<CInsertValuesContainer>(mDB, mID, mNumParameters, numReserveValues);
}



bool CDatabase::Open(const std::string& params)
{
    mConnection = PQconnectdb(params.c_str());
    if(mConnection == nullptr)
        return false;
    return (PQstatus(mConnection) != CONNECTION_BAD);
}

bool CDatabase::Close()
{
    if(mConnection)
    {
        PQfinish(mConnection);
        mConnection = nullptr;
    }
    return true;
}


bool CDatabase::ExecuteQuery(const std::string& query)
{
    CResult res(PQexec(mConnection, query.c_str()));
    return (res == true);
}

auto CDatabase::PrepareInsert(const std::shared_ptr<IDatabase>& db, const std::string& queryTpl, char wildcard) -> std::shared_ptr<IPreparedInsert>
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
    if(CResult(PQprepare(mConnection, id.c_str(), queryTplStream.str().c_str(), numParameters, nullptr)))
        preparedInsert = std::make_shared<CPreparedInsert>(db, std::move(id), numParameters);

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
