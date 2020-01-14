#pragma once

#include <memory>
#include <string>


class IDatabase;
class IPreparedInsert;

class IInsertValuesContainer
{
protected:
    std::shared_ptr<IDatabase> mDB;

public:
    IInsertValuesContainer(const std::shared_ptr<IDatabase>& db)
        : mDB(db)
    {}

    virtual ~IInsertValuesContainer() = default;

    virtual void AddValue(double value) = 0;
    virtual void AddValue(int value) = 0;
    virtual void AddValue(std::uint32_t value) = 0;
    virtual void AddValue(std::uint64_t value) = 0;
    virtual void AddValue(const std::string& value) = 0;
    virtual void AddValue(std::string&& value) = 0;

    virtual bool IsEmpty() const = 0;
    virtual bool IsMergingSupported() const = 0;
    virtual bool MergeIfPossible(std::unique_ptr<IInsertValuesContainer>& other) = 0;

    virtual auto GetSize() const -> std::size_t = 0;

    virtual auto InsertValues() -> std::size_t = 0;
};

class IPreparedInsert
{
protected:
    std::shared_ptr<IDatabase> mDB;
    std::size_t mNumParameters;

public:
    IPreparedInsert(const std::shared_ptr<IDatabase>& db, const std::size_t numParameters)
        : mDB(db), mNumParameters(numParameters)
    {}

    virtual ~IPreparedInsert() = default;

    virtual auto CreateValuesContainer(std::size_t numReserveValues=0) -> std::unique_ptr<IInsertValuesContainer> = 0;

    auto GetNumParameters() const -> std::size_t
    {return mNumParameters;}
};

class IDatabase
{
public:
    IDatabase() = default;
    IDatabase(const IDatabase&) = delete;
    IDatabase& operator=(const IDatabase&) = delete;
    IDatabase(const IDatabase&&) = delete;
    IDatabase& operator=(const IDatabase&&) = delete;

    virtual ~IDatabase() = default;

    virtual bool Open(const std::string& params) = 0;
    virtual bool Close() = 0;

    virtual bool ExecuteQuery(const std::string& query) = 0;
    virtual auto PrepareInsert(const std::shared_ptr<IDatabase>& db, const std::string& queryTpl, std::size_t numWildcards, char wildcard) -> std::shared_ptr<IPreparedInsert> = 0;

    virtual bool BeginTransaction() = 0;
    virtual bool CommitAndBeginTransaction() = 0;
    virtual bool EndTransaction() = 0;
};
