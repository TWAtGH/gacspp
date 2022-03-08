/**
 * @file   IDatabase.hpp
 * @brief  Contains the abstraction layer for database interaction
 *
 * @author Tobias Wegner
 * @date   March 2022
 * 
 * The IDatabase interface describes the methods required to interact with a database.
 * Once a database was opened, the database interface can be used to create IPreparedInsert
 * objects. These objects represent the schema and format that the database expects for inserting
 * values. An IPreparedInsert object can be used to create an IInsertValuesContainer, which can
 * be used by the simulation to add and store differently typed values.
 */

#pragma once

#include <memory>
#include <string>


class IDatabase;
class IPreparedInsert;


/**
* @brief Class that abstracts the writing of differently typed values to a insert statement
*/
class IInsertValuesContainer
{
protected:
    /**
    * @brief shared pointer to the associated database instance
    */
    std::shared_ptr<IDatabase> mDB;

public:
    /**
    * @brief constructs an object given the owning database instance
    * 
    * @param db shared pointer to the database instance
    */
    IInsertValuesContainer(const std::shared_ptr<IDatabase>& db)
        : mDB(db)
    {}

    virtual ~IInsertValuesContainer() = default;

    /**
    * @brief Adds a double to the insert statement
    *
    * @param value double typed value
    */
    virtual void AddValue(double value) = 0;

    /**
    * @brief Adds an int to the insert statement
    *
    * @param value int typed value
    */
    virtual void AddValue(int value) = 0;

    /**
    * @brief Adds an unsigned 4 byte int to the insert statement
    *
    * @param value unsigned 4 byte int typed value
    */
    virtual void AddValue(std::uint32_t value) = 0;

    /**
    * @brief Adds an unsigned 8 byte int to the insert statement
    *
    * @param value unsigned 8 byte int typed value
    */
    virtual void AddValue(std::uint64_t value) = 0;

    /**
    * @brief Adds a std::string to the insert statement
    *
    * @param value std::string value
    */
    virtual void AddValue(const std::string& value) = 0;

    /**
    * @brief Adds a std::string to the insert statement using move semantics
    *
    * @param value std::string value (will be consumed)
    */
    virtual void AddValue(std::string&& value) = 0;

    /**
    * @brief Checks if any values have been added yet
    * 
    * @return true if the container is still empty, false otherwise
    */
    virtual bool IsEmpty() const = 0;

    /**
    * @brief Checks if this container can be merged with another object of this type
    *
    * @return true if container of this type can be merged
    */
    virtual bool IsMergingSupported() const = 0;

    /**
    * @brief Merges another container into this container if possible
    * 
    * @param other the container to merge into this one
    *
    * @return true if the container were succesfully merged
    */
    virtual bool MergeIfPossible(std::unique_ptr<IInsertValuesContainer>& other) = 0;

    /**
    * @brief Gets the current number of values in the container
    * 
    * @return the number of values in this container
    */
    virtual auto GetSize() const -> std::size_t = 0;

    /**
    * @brief Insert the values into the database
    *
    * @return number of inserts that were executed
    */
    virtual auto InsertValues() -> std::size_t = 0;
};

/**
* @brief Represents a template of an insert query
*/
class IPreparedInsert
{
protected:
    /**
    * @brief shared pointer of the associated database instance
    */
    std::shared_ptr<IDatabase> mDB;

    /**
    * @brief Number of values that the insert query template expects
    */
    std::size_t mNumParameters;

public:
    /**
    * @brief constructs an object given the owning database instance
    *
    * @param db shared pointer to the database instance
    * @param numParams number of values that the insert query template expects
    */
    IPreparedInsert(const std::shared_ptr<IDatabase>& db, const std::size_t numParameters)
        : mDB(db), mNumParameters(numParameters)
    {}

    virtual ~IPreparedInsert() = default;

    /**
    * @brief Creates a new value container based on this insert statement template
    *
    * @param numReserveValues memory to reserve for value insertion in the container
    * 
    * @return unique pointer of the created value container
    */
    virtual auto CreateValuesContainer(std::size_t numReserveValues=0) -> std::unique_ptr<IInsertValuesContainer> = 0;

    auto GetNumParameters() const -> std::size_t
    {return mNumParameters;}
};

/**
* @brief Interface that abstracts the interaction with a database
*/
class IDatabase
{
public:
    IDatabase() = default;
    IDatabase(const IDatabase&) = delete;
    IDatabase& operator=(const IDatabase&) = delete;
    IDatabase(const IDatabase&&) = delete;
    IDatabase& operator=(const IDatabase&&) = delete;

    virtual ~IDatabase() = default;

    /**
    * @brief Opens a database given a paramter string.
    * 
    * @param params string containing options to open a database
    * 
    * @return true on success, false otherwise
    */
    virtual bool Open(const std::string& params) = 0;

    /**
    * @brief Closes a database.
    * 
    * @return true on success, false otherwise
    */
    virtual bool Close() = 0;

    /**
    * @brief Directly executes a query.
    * 
    * @param query query string to execute
    * 
    * @return true on success, false otherwise
    */
    virtual bool ExecuteQuery(const std::string& query) = 0;

    /**
    * @brief Prepares an insert statement for later usage.
    * 
    * @param db valid shared pointer to the database instance
    * @param queryTpl a template of the query containing wildcard characters at the places where values will be placed
    * @param numWildcards number of wildcards in the query
    * @param wildcard character used to indicate a wildcard
    * 
    * @return a shared pointer to an IPreparedInsert object or nullptr on failure
    */
    virtual auto PrepareInsert(const std::shared_ptr<IDatabase>& db, const std::string& queryTpl, std::size_t numWildcards, char wildcard) -> std::shared_ptr<IPreparedInsert> = 0;

    /**
    * @brief Begins a database transaction
    *
    * @return true on success, false otherwise
    */
    virtual bool BeginTransaction() = 0;

    /**
    * @brief Ends a transaction and starts a new one
    *
    * @return true on success, false otherwise
    */
    virtual bool CommitAndBeginTransaction() = 0;

    /**
    * @brief Ends and commits a database transaction
    *
    * @return true on success, false otherwise
    */
    virtual bool EndTransaction() = 0;
};
