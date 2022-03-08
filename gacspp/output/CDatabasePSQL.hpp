/**
 * @file   CDatabasePSQL.hpp
 * @brief  Contains the database implementation for a PostgreSQL database
 *
 * @author Tobias Wegner
 * @date   March 2022
 */
#pragma once

#ifdef WITH_PSQL

#include <libpq-fe.h>

#endif

#include "IDatabase.hpp"


namespace psql
{

class CDatabase;
class CPreparedInsert;


/**
* @brief An extra class wrapping the result of a lib-pq query
*/
class CResult
{
private:
#ifdef WITH_PSQL
    /**
    * @brief native pointer to the data representing the database result
    */
    PGresult* mResult;
public:

    /**
    * @brief Initialises the object
    * 
    * @param result the native lib-pq pointer representing the database result
    */
    CResult(PGresult* result);

    CResult(const CResult&) = delete;
    CResult& operator=(const CResult&) = delete;
    CResult(CResult&&);
    CResult& operator=(CResult&&);

    ~CResult();
#endif

    /**
    * @brief Operator allows casting the lib-pq result to bool
    * 
    * @return true on success, false otherwise
    */
    operator bool() const;

    /**
    * @brief Converts the lib-pq result to a human readable string
    * 
    * @return the human readable string
    */
    std::string str() const;
};


/**
* @brief PostgreSQL implementation of the IInsertValuesContainer
*/
class CInsertValuesContainer : public IInsertValuesContainer
{
private:
    /**
    * @brief lib-pq allows precompiling/caching a template query. mID references the according psql::CPreparedInsert::mID
    */
    std::string mID;

    /**
    * @brief number of values required for a single insert statement
    */
    std::size_t mNumParameters;

    /**
    * @brief string containing all added values
    */
    std::string mValues;

public:
    /**
    * @brief Initialises the object
    * 
    * @param db valid shared pointer to the associated database connection
    * @param id id of the psql::CPreparedInsert object owning this instance
    * @param numParameters number of values required for a single insert statement
    * @param numReserveValues number of values to reserver memory for
    */
    CInsertValuesContainer(const std::shared_ptr<IDatabase>& db, const std::string& id, std::size_t numParameters, std::size_t numReserveValues=0);

    /**
    * @brief Adds a double to the insert statement
    *
    * @param value double typed value
    */
    virtual void AddValue(double value);

    /**
    * @brief Adds an int to the insert statement
    *
    * @param value int typed value
    */
    virtual void AddValue(int value);

    /**
    * @brief Adds an unsigned 4 byte int to the insert statement
    *
    * @param value unsigned 4 byte int typed value
    */
    virtual void AddValue(std::uint32_t value);

    /**
    * @brief Adds an unsigned 8 byte int to the insert statement
    *
    * @param value unsigned 8 byte int typed value
    */
    virtual void AddValue(std::uint64_t value);

    /**
    * @brief Adds a std::string to the insert statement
    *
    * @param value std::string value
    */
    virtual void AddValue(const std::string& value);

    /**
    * @brief Adds a std::string to the insert statement using move semantics
    *
    * @param value std::string value (will be consumed)
    */
    virtual void AddValue(std::string&& value);

    /**
    * @brief Checks if any values have been added yet
    *
    * @return true if the container is still empty, false otherwise
    */
    virtual bool IsEmpty() const
    {return mValues.empty();}

    /**
    * @brief The psql implementation support mergin value containers
    *
    * @return true
    */
    virtual bool IsMergingSupported() const
    {return true;}

    /**
    * @brief Merges another container into this container if possible
    *
    * @param other the container to merge into this one
    *
    * @return true if the container were succesfully merged
    */
    virtual bool MergeIfPossible(std::unique_ptr<IInsertValuesContainer>& other);
    
    virtual auto GetSize() const -> std::size_t
    {return mValues.size();};

    /**
    * @brief Insert the values into the database
    *
    * @return number of inserts that were executed
    */
    virtual auto InsertValues() -> std::size_t;
};


/**
* @brief Implements the interface using template queries that are preprocessed by lib-pq. The preprocessed query can be addressed using mID.
*/
class CPreparedInsert : public IPreparedInsert
{
private:
    /** 
    * @brief ID that can be used to tell lib-pq, which prepared query should be executed
    */
    std::string mID;

public:
    /**
    * @brief Initialisese the object
    * 
    * @param db valid shared pointer to the database instance
    * @param id the id that can be used with the lib-pq API to execute this prepared query
    * @param numParameters the number of values required for a single insert statement
    */
    CPreparedInsert(const std::shared_ptr<IDatabase>& db, std::string&& id, std::size_t numParameters);

    /**
    * @brief Creates a new value container based on this insert statement template
    *
    * @param numReserveValues memory to reserve for value insertion in the container
    *
    * @return unique pointer of the created value container
    */
    virtual auto CreateValuesContainer(std::size_t numReserveValues=0) -> std::unique_ptr<IInsertValuesContainer>;
};


/**
* @brief Implements the PostgreSQL database interface
*/
class CDatabase : public IDatabase
{
private:

#ifdef WITH_PSQL
    /**
    * @brief Native pointer representing the lib-pq database connection 
    */
    PGconn* mConnection;
#endif

    /**
    * @brief Number of queries that have been prepared
    */
    std::size_t mNumPreparerdQueries = 0;

public:
    ~CDatabase();

    /**
    * @brief Opens a PostgreSQL database connection
    *
    * @param params connection string to open the database
    *
    * @return true on success, false otherwise
    */
    virtual bool Open(const std::string& params);

    /**
    * @brief Closes the database connection
    *
    * @return true on success, false otherwise
    */
    virtual bool Close();

    /**
    * @brief Directly executes a query at the database.
    *
    * @param query the query string to execute
    *
    * @return true on success, false otherwise
    */
    virtual bool ExecuteQuery(const std::string& query);

    /**
    * @brief Uses lib-pq to precompile/cache a template query
    *
    * @param db valid shared pointer to the database instance
    * @param queryTpl a template of the query containing wildcard characters at the places where values will be placed
    * @param numWildcards number of wildcards in the query
    * @param wildcard character used to indicate a wildcard
    *
    * @return a shared pointer to an IPreparedInsert object or nullptr on failure
    */
    virtual auto PrepareInsert(const std::shared_ptr<IDatabase>& db, const std::string& queryTpl, std::size_t numWildcards, char wildcard) -> std::shared_ptr<IPreparedInsert>;

    /**
    * @brief Begins a database transaction
    *
    * @return true on success, false otherwise
    */
    virtual bool BeginTransaction();

    /**
    * @brief Ends a transaction and starts a new one
    *
    * @return true on success, false otherwise
    */
    virtual bool CommitAndBeginTransaction();

    /**
    * @brief Ends and commits a database transaction
    *
    * @return true on success, false otherwise
    */
    virtual bool EndTransaction();

#ifdef WITH_PSQL
    auto GetConnection() -> PGconn*
    {return mConnection;}
#endif
};

}
