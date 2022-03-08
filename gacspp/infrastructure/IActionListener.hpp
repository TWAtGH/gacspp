/**
 * @file   IActionListener.hpp
 * @brief  Definition of the action interfaces
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * This file contains the definition of the action interfaces. This includes
 * IRucioActionListener, IStorageElementActionListener, and IReplicaPreRemoveListener
 *
 */
#pragma once

#include <memory>

#include "common/constants.h"

struct SFile;
struct SReplica;


/**
* @brief Interface containing functions that will be called on certain actions of CRucio
*/
class IRucioActionListener
{
public:
    virtual ~IRucioActionListener() = default;

    /**
    * @brief Called by CRucio after a new file was created
    *
    * @param file pointer to the new file that was created
    * @param now the time the file was created at
    */
    virtual void PostCreateFile(SFile* file, TickType now) = 0;

    /**
    * @brief Called by CRucio before a file will be removed
    *
    * @param file pointer to the file that will be removed
    * @param now the time the file will be removed at
    */
    virtual void PreRemoveFile(SFile* file, TickType now) = 0;
};


/**
* @brief Interface containing methods that will be called on actions of a storage element
*/
class IStorageElementActionListener
{
public:
    virtual ~IStorageElementActionListener() = default;

    /**
    * @brief Called after a replica was completed, i.e., a transfer finished and completed the replica
    *
    * @param replica the replica that was completed
    * @param now the time the replica was completed at
    */
    virtual void PostCompleteReplica(SReplica* replica, TickType now) = 0;

    /**
    * @brief Called after a new replica has been created at the associated storage element.
    *
    * @param replica the new replica that was created
    * @param now the time the replica was created at
    */
    virtual void PostCreateReplica(SReplica* replica, TickType now) = 0;

    /**
    * @brief Called right before a replica is going to be removed from its associated storage element
    *
    * @param replica the replica that is about to be removed
    * @param now the time the replica will be removed at
    */
    virtual void PreRemoveReplica(SReplica* replica, TickType now) = 0;
};


/**
* @brief Interface associated with a single replica that is called by a storage element when the associated replica is removed
* 
* An instance of this interface can be associated with at most one replica instance.
* Used by transfer managers to notify transfers in case the replica gets removed during a transfer.
*/
class IReplicaPreRemoveListener
{
public:
    virtual ~IReplicaPreRemoveListener() = default;

    /**
    * @brief Called by the storage element owning the replica before the replica is removed
    * 
    * @param replica the replica to be removed
    * @param now the time the replica is removed at
    * 
    * @return true when the interface instance should be kept in memory otherwise false
    */
    virtual bool PreRemoveReplica(SReplica* replica, TickType now) = 0;
};