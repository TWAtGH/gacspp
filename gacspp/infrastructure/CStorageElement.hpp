/**
 * @file   CStorageElement.hpp
 * @brief  Contains the CStorageElement implementation, which is based on the delegate design pattern
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * The CStorageElement class delegates its functionality to an object of type IStorageElementDelegate.
 * This allows the storage element to use different implementations while enabling sub-classes of CStorageElement
 * to dynamically chose their base class functionality.
 *
 */
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/constants.h"

class ISite;
class CNetworkLink;
struct SFile;
struct SReplica;

class IStorageElementDelegate;
class IStorageElementActionListener;


/**
* @brief Represents a storage element. Only basic functionality is implemented directly. The rest is delegated to the contained IStorageElementDelegate object.
*
* A storage element is the logical description of a certain storage area at an associated site. For example, a data centre could consist of tape storage and
* disk storage. Both represented by a different storage element.
* The key functionality of storage elements is the management of replicas, including their creation, deletion, and tracking their storage increase.
* Furthermore, storage elements allow being notified about operations, such as, new replica creation, data requests, or deletions. They also provide functionality
* to control and monitor the limitation of storage space.
* Finally, they are used to create network links.
*/
class CStorageElement
{
public:

    /**
    * @brief The types of operations that can be triggered on a storage element
    */
    enum OPERATION
    {
        INSERT,
        GET,
        CREATE_TRANSFER,
        DELETE,
        CUSTOM
    };

    /**
    * @brief Constructor to create a new storage element. Used by an ISite object
    * 
    * @param name name of the new storage element (string will be consumed)
    * @param site the site that this storage element belongs to and is owned by
    * @param allowDuplicateReplicas whether to allow the storage element to contain multiple replicas of the same file
    * @param limit the storage limit for this storage element
    */
    CStorageElement(std::string&& name, ISite* site, bool allowDuplicateReplicas = false, SpaceType limit = 0);


    /**
    * @brief Method that notifies the storage element about an executed operation, e.g., data access or deletion
    *
    * @param op operation that is being executed
    */
    virtual void OnOperation(OPERATION op);


    /**
    * @brief Method to create a new network link that originates from this storage element to the given destination storage element
    *
    * @param dstStorageElement valid pointer to the destination storage element
    * @param bandwidth bandwidth in bytes per second of the new network link
    * 
    * @return a pointer to the new network link. The pointer is valid as long as the source storage element is valid.
    */
    virtual auto CreateNetworkLink(CStorageElement* dstStorageElement, SpaceType bandwidth) -> CNetworkLink*;


    /**
    * @brief Method to create a new replica of the given file at this storage element. Replica will be empty.
    *
    * @param file pointer to the file of the new replica
    * @param now simulation time the replica was created at
    * 
    * @return pointer to the newly created replica. Can return nullptr depending on the delegate implementation, e.g., to prevent duplicates.
    * Pointer is valid as long as the replica lives. The storage element will call corresponding action interfaces on replica deletion.
    */
    virtual auto CreateReplica(SFile* file, TickType now) -> SReplica*;


    /**
    * @brief Removes a replica from the storage element, freeing the storage space it consumed.
    *
    * @param replica valid pointer to the replica that should be removed. Asserts if the given replica belongs to a different storage element
    * @param now the simulation time the replica was removed on
    * @param needLock uses a mutex to lock protect the internal data structures if deletion is done in different threads
    */
    virtual void RemoveReplica(SReplica* replica, TickType now, bool needLock = true);

    /**
    * @brief Notifies the storage element that the storage spaced consumed by the given replica was increased
    *
    * @param replica valid pointer to the replica that was increased
    * @param amount the amount that the replica was increased by
    * @param now the time at which the replica was increased
    */
    virtual void OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now);

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetSite() const -> ISite*
    {return mSite;}


    /**
    * @brief Getter to get a const reference to the internal array storing all replicas of the storage element (copying could be expensive)
    * 
    * @return a const reference to the std::vector storing the replicas
    */
    auto GetReplicas() const -> const std::vector<std::unique_ptr<SReplica>>&;

    /**
    * @brief Getter to get a const reference to the internal array storing the networklinks going out of this storage element
    * 
    * @return a const reference to the std::vector storing the outgoing network links
    */
    inline auto GetNetworkLinks() const -> const std::vector<std::unique_ptr<CNetworkLink>>&
    {return mNetworkLinks;}


    /**
    * @brief Helper method to get a networklink connection this storage element with a given destination storage element
    *
    * @param dstStorageElement the destination storage element
    * 
    * @return a pointer to the desired CNetworkLink object if found or nullptr otherwise
    */
    auto GetNetworkLink(const CStorageElement* dstStorageElement) const -> CNetworkLink*;

    /**
    * @brief Getter for the amount of storage space used by this storage element
    * 
    * @return the storage space used by this storage element
    * 
    * When a new replica is created, it will allocate storage the size of its file. As the replica is increased by a transfer,
    * it increases the used storage and decreases the allocated storage by the same amount. This makes sure that a replica
    * that is successfully created can be fully transferred to the storage element.
    */
    auto GetUsedStorage() const -> SpaceType;

    /**
    * @brief Getter for the amount of storage space allocated at this storage element
    *
    * @return the storage space allocated at this storage element
    * 
    * When a new replica is created, it will allocate storage the size of its file. As the replica is increased by a transfer,
    * it increases the used storage and decreases the allocated storage by the same amount. This makes sure that a replica
    * that is successfully created can be fully transferred to the storage element.
    */
    auto GetAllocatedStorage() const -> SpaceType;

    /**
    * @brief Getter for the maximum of storage that can used
    *
    * @return the storage space limit for this storage element. 0 means the storage is unlimited.
    */
    auto GetLimit() const -> SpaceType;


    /**
    * @brief Calculates the ratio between used storage and the storage limit
    *
    * @return the ratio between used storage and the storage limit.
    * 0 means the storage is unlimited. 1 means the storage is fully used.
    */
    auto GetUsedStorageLimitRatio() const -> double;


    /**
    * @brief Indicates if the given volume can be allocated at this storage element
    * 
    * @param the desired volume to store
    *
    * @return true if the passed volume can be allocated, false otherwise
    */
    bool CanStoreVolume(SpaceType volume) const;


    /**
    * @brief The action listener interface implementations associated with this storage element instance
    */
    std::vector<IStorageElementActionListener*> mActionListener;


    /**
    * @brief A random value generator for the access latency of this storage element
    */
    std::unique_ptr<class IValueGenerator> mAccessLatency;
    
private:

    /**
    * @brief Unique id of this storage element. Id is unique across all object types
    */
    IdType mId;

    /**
    * @brief Name of the storage element
    */
    std::string mName;

    /**
    * @brief std::vector containing the network links originating from and owned by this storage element
    */
    std::vector<std::unique_ptr<CNetworkLink>> mNetworkLinks;

protected:

    /**
    * @brief pointer to the site that owns this storage element
    */
    ISite* mSite;

    /**
    * @brief pointer to the delegate implementing the functionality of this storage element
    */
    std::unique_ptr<IStorageElementDelegate> mDelegate;


    /**
    * @brief maps the destination storage element id to a network link. Allows for faster look ups of GetNetworkLink()
    */
    std::unordered_map<IdType, std::size_t> mDstStorageElementIdToNetworkLinkIdx;
};



/**
* @brief Delegate definition declaring the methods required to implement a storage element delegate. See CStorageElement for details.
*/
class IStorageElementDelegate
{
public:

    /**
    * @brief Constructor takes the associated storage element object and the configured storage limit
    */
    IStorageElementDelegate(CStorageElement* storageElement, SpaceType limit = 0);

    IStorageElementDelegate(IStorageElementDelegate&&) = delete;
    IStorageElementDelegate& operator=(IStorageElementDelegate&&) = delete;
    IStorageElementDelegate(const IStorageElementDelegate&) = delete;
    IStorageElementDelegate& operator=(const IStorageElementDelegate&) = delete;

    virtual ~IStorageElementDelegate();


    /**
    * @brief Called by CStorageElement. Must implement OnOperation logic
    */
    virtual void OnOperation(CStorageElement::OPERATION op) = 0;

    /**
    * @brief Called by CStorageElement. Must implement CreateReplica logic
    */
    virtual auto CreateReplica(SFile* file, TickType now)->SReplica* = 0;

    /**
    * @brief Called by CStorageElement. Must implement RemoveReplica logic
    */
    virtual void RemoveReplica(SReplica* replica, TickType now, bool needLock = true) = 0;

    /**
    * @brief Called by CStorageElement. Must implement OnIncreaseReplica logic
    */
    virtual void OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now) = 0;

    inline auto GetReplicas() const -> const std::vector<std::unique_ptr<SReplica>>&
    {return mReplicas;}

    inline auto GetStorageElement() const -> CStorageElement*
    {return mStorageElement;}

    inline auto GetUsedStorage() const -> SpaceType
    {return mUsedStorage;}
    inline auto GetAllocatedStorage() const -> SpaceType
    {return mAllocatedStorage;}
    inline auto GetLimit() const -> SpaceType
    {return mLimit;}
    inline auto GetUsedStorageLimitRatio() const -> double
    {return (mLimit > 0) ? static_cast<double>(mUsedStorage) / mLimit : 0;}
    inline bool CanStoreVolume(SpaceType volume) const
    {return (mLimit > 0) ? ((mUsedStorage + mAllocatedStorage + volume) <= mLimit) : true;}

protected:
    /**
    * @brief Array of replicas owned by this storage element.
    */
    std::vector<std::unique_ptr<SReplica>> mReplicas;

    /**
    * @brief Associated CStorageElement object
    */
    CStorageElement *mStorageElement;

    /**
    * @brief Storage that is used by replicas
    */
    SpaceType mUsedStorage = 0;

    /**
    * @brief Storage that is allocated by (incomplete) replicas but not yet used
    */
    SpaceType mAllocatedStorage = 0;

    /**
    * @brief Storage space limit of this storage element. 0 means unlimited
    */
    SpaceType mLimit;
};



/**
* @brief Base implementation of a storage element delegate.
* This delegate does not prevent the creation of multiple replicas for the same file.
*/
class CBaseStorageElementDelegate : public IStorageElementDelegate
{
public:
    using IStorageElementDelegate::IStorageElementDelegate;


    /**
    * @brief This function is a no-op for this implementation
    */
    void OnOperation(CStorageElement::OPERATION op) override;


    /**
    * @brief Creates a new SReplica object owned by this storage element delegate
    * 
    * @param file the file the replica will be associated with
    * @param now the time the replica was created at
    * 
    * @return a pointer to the newly created replica or nullptr if the storage could not be allocated
    */
    auto CreateReplica(SFile* file, TickType now) -> SReplica* override;

    /**
    * @brief Removes a replica from the storage element, freeing the storage space it used and allocated. Calls action interfaces.
    *
    * @param replica valid pointer to the replica that should be removed. Asserts if the given replica belongs to a different storage element
    * @param now the simulation time the replica was removed on
    * @param needLock uses a mutex to lock protect the internal data structures if deletion is done in different threads
    */
    void RemoveReplica(SReplica* replica, TickType now, bool needLock = true) override;

    /**
    * @brief Notifies the storage element that the storage spaced consumed by the given replica was increased
    *
    * @param replica valid pointer to the replica that was increased
    * @param amount the amount that the replica was increased by
    * @param now the time at which the replica was increased
    */
    void OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now) override;

protected:
    /**
    * @brief mutex to lock the internal data structures in case operations are done in different threads
    */
    std::mutex mReplicaRemoveMutex;
};



/**
* @brief This delegate specification prevents the creation of multiple replicas from the same file at the same storage element.
*/
class CUniqueReplicaStorageElementDelegate : public CBaseStorageElementDelegate
{
public:
    using CBaseStorageElementDelegate::CBaseStorageElementDelegate;


    /**
    * @brief Creates a new SReplica object owned by this storage element delegate
    *
    * @param file the file the replica will be associated with
    * @param now the time the replica was created at
    *
    * @return the result of CBaseStorageElementDelegate::CreateReplica() or nullptr if the replica already exists at this storage element
    */
    auto CreateReplica(SFile* file, TickType now) -> SReplica* override;
};
