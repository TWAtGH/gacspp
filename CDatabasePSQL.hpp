#pragma once

#include <vector>

#include <libpq-fe.h>

#include "IDatabase.hpp"


namespace psql
{

class CResult
{
private:
    PGresult* mResult;

public:
    CResult(PGresult* result);

    CResult(const CResult&) = delete;
    CResult& operator=(const CResult&) = delete;
    CResult(CResult&&);
    CResult& operator=(CResult&&);

    ~CResult();

    operator bool() const;
};

class CDatabase;
class CPreparedInsert;

class CInsertValuesContainer : public IInsertValuesContainer
{
private:
    std::string mID;
    std::size_t mNumParameters;
    std::vector<std::string> mValues;

public:
    CInsertValuesContainer(const std::shared_ptr<IDatabase>& db, const std::string& id, std::size_t numParameters, std::size_t numReserveValues=0);

    virtual void AddValue(double value);
    virtual void AddValue(int value);
    virtual void AddValue(std::uint32_t value);
    virtual void AddValue(std::uint64_t value);
    virtual void AddValue(const std::string& value);
    virtual void AddValue(std::string&& value);

    virtual auto InsertValues() -> std::size_t;
};

class CPreparedInsert : public IPreparedInsert
{
private:
    std::string mID;

public:
    CPreparedInsert(const std::shared_ptr<IDatabase>& db, std::string&& id, std::size_t numParameters);

    virtual auto CreateValuesContainer(std::size_t numReserveValues=0) -> std::unique_ptr<IInsertValuesContainer>;
};

class CDatabase : public IDatabase
{
private:
    PGconn* mConnection;
    std::size_t mNumPreparerdQueries = 0;


public:
    virtual bool Open(const std::string& params);
    virtual bool Close();

    virtual bool ExecuteQuery(const std::string& query);
    virtual auto PrepareInsert(const std::shared_ptr<IDatabase>& db, const std::string& queryTpl, char wildcard) -> std::shared_ptr<IPreparedInsert>;

    virtual bool BeginTransaction();
    virtual bool CommitAndBeginTransaction();
    virtual bool EndTransaction();

    auto GetConnection() -> PGconn*
    {return mConnection;}
};

}
