/**
 * @file   SFile.hpp
 * @brief  Contains the data structures for simulated files and replicas.
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * The primary structures contained in this file are SFile to represent simulated files and
 * SReplica to represent simulated replicas. Furthermore, the file contains the SIndexedReplica
 * structure that stores replicas and allows index based access with fast insertion and deletion.
 */
#pragma once

#include <memory>
#include <vector>

#include "common/constants.h"

class CStorageElement;
class IReplicaPreRemoveListener;
struct SReplica;

/**
* @brief Structure representing a simulated file
*
* A simulated file is primarily described by an id, the size, the creation time, and the life time.
*/
struct SFile
{

    /**
    * @brief Initialises a new file object with the given data
    *
    * @param size the file size
    * @param createdAt the time the file is created at
    * @param lifetime the duration the file is expected to live for initially
    * @param indexAtRucio the index of CRucio::mFiles that refers to this file instance. Allow fast access for certain operations
    */
    SFile(SpaceType size, TickType createdAt, TickType lifetime, std::size_t indexAtRucio);


    /**
    * @brief Called by a storage element after a new replica of this file was created.
    *
    * @param replica valid pointer to the created replica object
    * 
    * This methods stores a pointer of the replica in SFile::mReplicas to provide optimised access
    * to the replicas of a file.
    */
    void PostCreateReplica(SReplica* replica);

    /**
    * @brief Called by a storage element right before a replica of this file will be removed
    *
    * @param replica valid pointer to the replica object that will be removed
    *
    * This methods removes the pointer of the replica in SFile::mReplicas.
    */
    void PreRemoveReplica(const SReplica* replica);

    /**
    * @brief Extends the expiration time of this file
    *
    * @param newExpiresAt the new expiration time of the file. Can only be increased.
    */
    void ExtendExpirationTime(TickType newExpiresAt);

    /**
    * @brief Searches a replica of this file at a given storage element.
    *
    * @param storageElement a valid pointer to the storage element of the desired replica
    *
    * @return a pointer to a SReplica instance if a replica exists at the given storage element or nullptr otherwise
    */
    auto GetReplicaByStorageElement(const CStorageElement* storageElement) const -> SReplica*;

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetCreatedAt() const -> TickType
    {return mCreatedAt;}
    inline auto GetSize() const -> SpaceType
    {return mSize;}
    inline auto GetReplicas() const -> const std::vector<SReplica*>&
    {return mReplicas;}


    /**
    * @brief The index of CRucio::mFiles that refers to this file instance. Allow fast access for certain operations
    */
    std::size_t mIndexAtRucio;

    /**
    * @brief The expiration time of this file
    */
    TickType mExpiresAt;

    /**
    * @brief The popularity of this file
    * 
    * todo: Maybe this should be moved to the corresponding transfer generator
    */
    std::uint32_t mPopularity = 1;

private:
    /**
    * @brief The unique id of this file. The id is unique across all object types.
    * 
    * The id is automatically generated in the constructor.
    */
    IdType mId;

    /**
    * @brief The time stamp the file was created at.
    */
    TickType mCreatedAt;

    /**
    * @brief The file size
    */
    SpaceType mSize;

    /**
    * @brief An array containing a reference to each replica of this file.
    */
    std::vector<SReplica*> mReplicas;
};


/**
* @brief Structure representing a simulated replica
*
* A simulated replica is primarily described by an id, the associated file, the associated storage element,
* the current size (currently used storage), and the creation time stamp.
*/
struct SReplica
{
    /**
    * @brief Initialises a new replica object using the given values
    * 
    * @param file a valid pointer to the SFile object that this replica belongs to
    * @param storageElement a valid pointer to the CStorageElement object that owns this replica
    * @param createdAt the time stamp that this replica was created at
    * @param indexAtStorageElement the index of IStorageElementDelegate::mReplicas that refers to this replica instance.
    *        Allow fast access for certain operations
    * 
    */
    SReplica(SFile* file, CStorageElement* storageElement, TickType createdAt, std::size_t indexAtStorageElement);


    /**
    * @brief Increases the current size (mCurSize) of the replica.
    *
    * @param amount the amount by which the replica should be increased
    * @param now the current simulation time stamp
    *
    * @return the real amount the replica was increased by. mCurSize can not become larger than the file size.
    */
    auto Increase(SpaceType amount, TickType now) -> SpaceType;

    /**
    * @brief Extends the expiration time of this replica instance. Also corrects the expiration time of the associated file if necessary.
    *
    * @param newExpiresAt new expiration time of the replica. Can only be increased.
    */
    void ExtendExpirationTime(TickType newExpiresAt);

    /**
    * @brief Check if the replica is complete or incomplete
    *
    * @return true if the replica is complete, i.e., mCurSize reached the size of the associated file. False otherwise.
    */
    inline bool IsComplete() const
    {return mCurSize == mFile->GetSize();}

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetCreatedAt() const -> TickType
    {return mCreatedAt;}
    inline auto GetCurSize() const -> SpaceType
    {return mCurSize;}

    inline auto GetFile() const -> SFile*
    {return mFile;}
    inline auto GetStorageElement() const -> CStorageElement*
    {return mStorageElement;}

public:
    /**
    * @brief The action interface to invoke prior to the removal of this replica. See IReplicaPreRemoveListener.
    */
    IReplicaPreRemoveListener* mRemoveListener = nullptr;

    /**
    * @brief The index of IStorageElementDelegate::mReplicas that refers to this replica instance. Allow fast access for certain operations
    */
    std::size_t mIndexAtStorageElement;

    /**
    * @brief The expiration time stamp of this replica.
    */
    TickType mExpiresAt;

    /**
    * @brief The number of objects that this replica is used for. Can be removed if it reaches 0.
    * 
    * todo: maybe should be moved to the corresponding transfer generators
    */
    std::uint32_t mUsageCounter = 0;

private:
    /**
    * @brief The unique id of this replica. The id is unique across all object types.
    * 
    * The id is automatically generated in the constructor.
    */
    IdType mId;

    /**
    * @brief The creation time stamp of this replica.
    */
    TickType mCreatedAt;

    /**
    * @brief The pointer to the file object associated with this replica.
    */
    SFile* mFile;

    /**
    * @brief The pointer to the CStorageElement object that owns this replica.
    */
    CStorageElement* mStorageElement;

    /**
    * @brief The current size of this replica. Can not become larger than the file size.
    */
    SpaceType mCurSize = 0;
};


/**
* @brief Helper structure storing indexed SReplica pointers
* 
* This structure store SReplica pointers and provides index based access and fast
* insertion and deletion at the cost of extra memory. Internally uses a hash map
* to associated a replica pointer to its index and a vector to provide the index
* based access.
*/
struct SIndexedReplicas
{
private:
    /**
    * @brief Hash map that maps a SReplica pointer to its index in the mReplicas array
    */
    std::unordered_map<SReplica*, std::size_t> mReplicaToIdx;

    /**
    * @brief Array that stores all SReplica pointers of this structure
    */
    std::vector<SReplica*> mReplicas;

public:
    inline auto IsEmpty() const -> std::size_t
    {return mReplicas.empty();}

    inline auto NumReplicas() const -> std::size_t
    {return mReplicas.size();}
    
    /**
    * @brief Get a replica by an index
    * 
    * @param idx the index
    * 
    * @return the SReplica pointer stored at the given index
    */
    inline auto GetReplica(std::size_t idx) const -> SReplica*
    {return mReplicas[idx];}
    

    /**
    * @brief Checks whether the structure contains a given SReplica pointer
    * 
    * @param replica the SReplica pointer to check
    * 
    * @return true if the pointer is stored, false otherwise
    */
    inline bool HasReplica(SReplica* replica) const
    {return (mReplicaToIdx.count(replica) > 0);}


    /**
    * @brief Adds a replica pointer to the structure
    *
    * @param replica the SReplica pointer to add
    *
    * @return true if the pointer was added, false if the pointer already exists
    */
    bool AddReplica(SReplica* replica);

    /**
    * @brief Removes a replica pointer from the structure
    *
    * @param replica the SReplica pointer to remove
    *
    * @return true if the pointer was removed, false if it does not exist
    */
    bool RemoveReplica(SReplica* replica);

    /**
    * @brief Removes a replica index from the structure
    *
    * @param idx the index to remove
    *
    * @return true if the index could be removed, false otherwise
    */
    bool RemoveReplica(std::size_t idx);

    /**
    * @brief Removes and returns the last replica from the array
    *
    * @return the pointer of the removed replica
    */
    auto ExtractBack() -> SReplica*;
};