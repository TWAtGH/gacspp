/**
 * @file   TransferManager.hpp
 * @brief  Contains the two default provided transfer managers
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * The CBaseTransferManager provides a common base class for transfer managers.
 * The first default implemented transfer managers is implemented in CTransferManager and is based
 * on the network link bandwidth. The other default implemented transfer manager is implemented in
 * CFixedTimeTransferManager and is based on duration based transfers.
 */

#pragma once

#include <forward_list>
#include <list>
#include <map>
#include <unordered_map>

#include "CScheduleable.hpp"
#include "infrastructure/IActionListener.hpp"

class IPreparedInsert;
class CNetworkLink;
struct SFile;
struct SReplica;


/**
* @brief Specialisation of the IReplicaPreRemoveListener that broadcasts the event to multiple child listeners
*/
class CReplicaPreRemoveMultiListener : public IReplicaPreRemoveListener
{
public:
    /**
    * @brief List of listeners the event will be broadcasted to
    */
    std::forward_list<IReplicaPreRemoveListener*> mListener;


    /**
    * @brief Called by the storage element owning the replica before the replica is removed
    *
    * @param replica the replica to be removed
    * @param now the time the replica is removed at
    *
    * @return true when the interface instance should be kept in memory otherwise false
    */
    virtual bool PreRemoveReplica(SReplica* replica, TickType now) override;
};


/**
* @brief Base class for the default implemented transfer managers.
*/
class CBaseTransferManager : public CSchedulable
{
public:
    using CSchedulable::CSchedulable;

    /**
    * @brief Number of transfers completed by this transfer manager
    */
    std::uint32_t mNumCompletedTransfers = 0;

    /**
    * @brief Number of failed transfers
    */
    std::uint32_t mNumFailedTransfers = 0;

    /**
    * @brief Sum of the duration of all completed transfers. Used to calculate mean transfer duration
    */
    TickType mSummedTransferDuration = 0;


    /**
    * @brief Must be implemented in subclasses to return the number of active transfers
    * 
    * @return number of currently active transfers
    */
    virtual auto GetNumActiveTransfers() const -> std::size_t = 0;
};


/**
* @brief Default transfer manager based on (shared) network link bandwidth
*/
class CTransferManager : public CBaseTransferManager
{
private:
    /**
    * @brief Query used to write transfers to the output system
    */
    std::shared_ptr<IPreparedInsert> mOutputTransferInsertQuery;

    /**
    * @brief Last simulation time stamp the transfer manager event was called
    */
    TickType mLastUpdated = 0;

    /**
    * @brief Frequency the transfer manager should be called at
    */
    TickType mTickFreq;

    /**
    * @brief Internal data structure representing a transfer for CTransferManager
    */
    struct STransfer : public IReplicaPreRemoveListener
    {
        /**
        * @brief Source replica
        */
        SReplica* mSrcReplica;

        /**
        * @brief Destination replica
        */
        SReplica* mDstReplica;

        /**
        * @brief Used network link
        */
        CNetworkLink* mNetworkLink;

        /**
        * @brief Simulation time stamp the transfer was queued at
        */
        TickType mQueuedAt;

        /**
        * @brief Simulation time stamp the transfer was activated at
        */
        TickType mActivatedAt;

        /**
        * @brief Simulation time stamp the transfer was started at
        */
        TickType mStartAt;

        /**
        * @brief If true, the source replica will be deleted after transfer completion
        */
        bool mDeleteSrcReplica;

        STransfer(  SReplica* srcReplica,
                    SReplica* dstReplica,
                    CNetworkLink* networkLink,
                    TickType queuedAt,
                    TickType startAt,
                    bool deleteSrcReplica);
        ~STransfer();

        /**
        * @brief Implements the IReplicaPreRemoveListener interface to fail this transfer in case a replica gets deleted.
        * 
        * @param replica replica object that gets deleted
        * @param now current simulation time
        *
        * @return true when the interface instance should be kept in memory otherwise false
        */
        bool PreRemoveReplica(SReplica* replica, TickType now) override;
    };

    /**
    * @brief Currently active transfers sorted by starting time.
    */
    std::multimap<TickType, std::unique_ptr<STransfer>> mActiveTransfers;

    /**
    * @brief Maps a network link object to a list of queued transfers
    */
    std::unordered_map<CNetworkLink*, std::list<std::unique_ptr<STransfer>>> mQueuedTransfers;

public:
    /**
    * @brief Initialises the transfer manager
    * 
    * @param tickFreq the desired frequency in which the transfer manager should be executed
    * @param startTick the first time point this manager will tick
    */
    CTransferManager(TickType tickFreq, TickType startTick = 0);

    /**
    * @brief Activate queued transfers when possible and update active transfers. Called by the event loop.
    * 
    * @param now current simulation time
    */
    void OnUpdate(TickType now) final;


    /**
    * @brief Called by the simulation engine if the simulation stops
    * 
    * @param now current simulation time
    */
    void Shutdown(const TickType now) final;

    /**
    * @brief Creates and queues a new transfer.
    * 
    * @param srcReplica source replica of the transfer
    * @param dstReplica destination replica of the transfer
    * @param now current simulation time
    * @param deleteSrcReplica if true the source replica will be deleted on transfer completion
    */
    void CreateTransfer(SReplica* srcReplica, SReplica* dstReplica, TickType now, bool deleteSrcReplica = false);

    auto GetNumActiveTransfers() const -> std::size_t
    {return mActiveTransfers.size();}
};


/**
* @brief Transfer manager implementation that uses duration based transfer updates
*/
class CFixedTimeTransferManager : public CBaseTransferManager
{
private:
    /**
    * @brief Query used to write transfers to the output system
    */
    std::shared_ptr<IPreparedInsert> mOutputTransferInsertQuery;

    /**
    * @brief Last simulation time stamp the transfer manager event was called
    */
    TickType mLastUpdated = 0;

    /**
    * @brief Frequency the transfer manager should be called at
    */
    TickType mTickFreq;

    /**
    * @brief Internal data structure representing a transfer for CFixedTimeTransferManager
    */
    struct STransfer : public IReplicaPreRemoveListener
    {
        /**
        * @brief Source replica
        */
        SReplica* mSrcReplica;

        /**
        * @brief Destination replica
        */
        SReplica* mDstReplica;

        /**
        * @brief Used network link
        */
        CNetworkLink* mNetworkLink;

        /**
        * @brief Simulation time stamp the transfer was queued at
        */
        TickType mQueuedAt;

        /**
        * @brief Simulation time stamp the transfer was started at
        */
        TickType mStartAt;

        /**
        * @brief Amount each replica will be increased per tick
        */
        SpaceType mIncreasePerTick;

        STransfer(  SReplica* srcReplica,
                    SReplica* dstReplica,
                    CNetworkLink* networkLink,
                    TickType queuedAt,
                    TickType startAt,
            SpaceType increasePerTick);
        ~STransfer();

        /**
        * @brief Implements the IReplicaPreRemoveListener interface to fail this transfer in case a replica gets deleted.
        *
        * @param replica replica object that gets deleted
        * @param now current simulation time
        *
        * @return true when the interface instance should be kept in memory otherwise false
        */
        bool PreRemoveReplica(SReplica* replica, TickType now) override;
    };

    /**
    * @brief Array containing all currently active transfers
    */
    std::vector<std::unique_ptr<STransfer>> mActiveTransfers;

    /**
    * @brief Array containing all queued transfers
    */
    std::vector<std::unique_ptr<STransfer>> mQueuedTransfers;

public:
    /**
    * @brief Initialises the transfer manager
    *
    * @param tickFreq the desired frequency in which the transfer manager should be executed
    * @param startTick the first time point this manager will tick
    */
    CFixedTimeTransferManager(TickType tickFreq, TickType startTick=0);

    /**
    * @brief Activate queued transfers when possible and update active transfers. Called by the event loop.
    *
    * @param now current simulation time
    */
    void OnUpdate(TickType now) final;

    /**
    * @brief Creates and queues a new transfer.
    *
    * @param srcReplica source replica of the transfer
    * @param dstReplica destination replica of the transfer
    * @param now current simulation time
    * @param startDelay delay time to wait before the transfer will be started
    * @param duration duration of the transfer
    */
    void CreateTransfer(SReplica* srcReplica, SReplica* dstReplica, TickType now, TickType startDelay, TickType duration);


    inline auto GetNumQueuedTransfers() const -> std::size_t
    {return mQueuedTransfers.size();}
    auto GetNumActiveTransfers() const -> std::size_t
    {return mActiveTransfers.size();}
};