/**
 * @file   CDatabaseDummy.hpp
 * @brief  Contains a dummy database implementation, which does not really output the data and just returns success.
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * Useful for testing
 */

#pragma once

#include <sstream>

#include "IDatabase.hpp"

namespace dummydb
{
/**
* @brief Implementation of the interface with empty functions doing nothing
*/
class CInsertValuesContainer : public IInsertValuesContainer
{
public:
    using IInsertValuesContainer::IInsertValuesContainer;

    virtual void AddValue(double value);
    virtual void AddValue(int value);
    virtual void AddValue(std::uint32_t value);
    virtual void AddValue(std::uint64_t value);
    virtual void AddValue(const std::string& value);
    virtual void AddValue(std::string&& value);

    virtual bool IsEmpty() const
    {return true;};
    virtual bool IsMergingSupported() const
    {return false;};
    virtual bool MergeIfPossible(std::unique_ptr<IInsertValuesContainer>& other);

    virtual auto GetSize() const -> std::size_t
    {return 0;};

    virtual auto InsertValues() -> std::size_t;
};

/**
* @brief Implementation that returns a dummydb::CInsertValuesContainer
*/
class CPreparedInsert : public IPreparedInsert
{
public:
    using IPreparedInsert::IPreparedInsert;

    virtual auto CreateValuesContainer(std::size_t numReserveValues=0) -> std::unique_ptr<IInsertValuesContainer>;
};


/**
* @brief Dummy database that returns success but does nothing in the background
*/
class CDatabase : public IDatabase
{
public:
    virtual bool Open(const std::string& params);
    virtual bool Close();

    virtual bool ExecuteQuery(const std::string& query);
    virtual auto PrepareInsert(const std::shared_ptr<IDatabase>& db, const std::string& queryTpl, std::size_t numWildcards, char wildcard) -> std::shared_ptr<IPreparedInsert>;

    virtual bool BeginTransaction();
    virtual bool CommitAndBeginTransaction();
    virtual bool EndTransaction();
};

}
