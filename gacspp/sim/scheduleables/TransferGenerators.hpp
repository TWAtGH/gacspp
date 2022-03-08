/**
 * @file   TransferGenerators.hpp
 * @brief  Contains the classes of the available transfer generators
 *
 * @author Tobias Wegner
 * @date   March 2022
 */

#pragma once

#include <forward_list>
#include <list>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "CScheduleable.hpp"

#include "infrastructure/IActionListener.hpp"
#include "infrastructure/SFile.hpp"

class IBaseSim;
class CNetworkLink;
class IValueGenerator;
class CTransferManager;
class CFixedTimeTransferManager;
class IPreparedInsert;
class IInsertValuesContainer;
class CStorageElement;

/**
* @brief Action interface implementation that automates the insertion of files and replicas in the output system before they are deleted.
*/
class CBaseOnDeletionInsert : public IRucioActionListener, public IStorageElementActionListener
{
protected:
    /**
    * @brief value container used to write files to the output system prior to their deletion
    */
    std::unique_ptr<IInsertValuesContainer> mFileValueContainer;

    /**
    * @brief value container used to write replicas to the output system prior to their deletion
    */
    std::unique_ptr<IInsertValuesContainer> mReplicaValueContainer;

    /**
    * @brief query used to generate the mFileValueContainer instance
    */
    std::shared_ptr<IPreparedInsert> mFileInsertQuery;

    /**
    * @brief query used to generate the mReplicaValueContainer instance
    */
    std::shared_ptr<IPreparedInsert> mReplicaInsertQuery;


    /**
    * @brief Add a file to the output prior to its deletion
    * 
    * @param file valid pointer to the file object
    */
    void AddFileDelete(SFile* file);

    /**
    * @brief Add a replica to the output prior to its deletion
    *
    * @param replica valid pointer to the replica object
    */
    void AddReplicaDelete(SReplica* replica);

public:
    CBaseOnDeletionInsert();

    /**
    * @brief Interface implementation called after a file was created. Does nothing.
    */
    void PostCreateFile(SFile* file, TickType now) override;

    /**
    * @brief Interface implementation called before a file is removed. Adds it to the output container.
    * 
    * @param file a valid pointer to the file object
    * @param now current simulation time
    */
    void PreRemoveFile(SFile* file, TickType now) override;

    /**
    * @brief Interface implementation called after a replica was completed. Does nothing.
    */
    void PostCompleteReplica(SReplica* replica, TickType now) override;

    /**
    * @brief Interface implementation called after a replica was created. Does nothing.
    */
    void PostCreateReplica(SReplica* replica, TickType now) override;

    /**
    * @brief Interface implementation called before a replica is removed. Adds it to the output container.
    *
    * @param replica a valid pointer to the replica object
    * @param now current simulation time
    */
    void PreRemoveReplica(SReplica* replica, TickType now) override;
};



/**
* @brief Optimises the CBaseOnDeletionInsert by buffering the data and regularly flushing them
*/
class CBufferedOnDeletionInsert : public CBaseOnDeletionInsert
{
private:
    /**
    * @brief Flushes the buffered deleted files data
    */
    void FlushFileDeletes();

    /**
    * @brief Flushes the buffered deleted replica data
    */
    void FlushReplicaDeletes();

public:
    virtual ~CBufferedOnDeletionInsert();

    /**
    * @brief Interface implementation called before a file is removed. Adds it to the buffer. Flushes if necessary.
    *
    * @param file a valid pointer to the file object
    * @param now current simulation time
    */
    void PreRemoveFile(SFile* file, TickType now) override;

    /**
    * @brief Interface implementation called before a replica is removed. Adds it to the buffer. Flushes if necessary.
    *
    * @param replica a valid pointer to the replica object
    * @param now current simulation time
    */
    void PreRemoveReplica(SReplica* replica, TickType now) override;
};



/**
* @brief Transfer generator implementing the HCDC model
*/
class CHCDCTransferGen : public CSchedulable, public IStorageElementActionListener
{
private:
    /**
    * @brief pointer to the associated simulation engine
    */
    IBaseSim* mSim;

    /**
    * @brief pointer to the object managing the transfers of this generator
    */
    std::shared_ptr<CTransferManager> mTransferMgr;

    /**
    * @brief the frequency in which this event should be called. 0 means just once
    */
    TickType mTickFreq;

    /**
    * @brief last time the event was called
    */
    TickType mLastUpdateTime = 0;

    /**
    * @brief Query used to write the input traces to the output system
    */
    std::shared_ptr<IPreparedInsert> mInputTraceInsertQuery;

    /**
    * @brief Query used to write the job traces to the output system
    */
    std::shared_ptr<IPreparedInsert> mJobTraceInsertQuery;

    /**
    * @brief Query used to write the output traces to the output system
    */
    std::shared_ptr<IPreparedInsert> mOutputTraceInsertQuery;

public:
    /**
    * @brief Helper struct to describe a job and determine its state
    */
    struct SJobInfo
    {
        /**
        * @brief Unique ID of a job. ID is unique across all object types.
        */
        IdType mJobId;

        /**
        * @brief Simulation time the job was created at.
        */
        TickType mCreatedAt = 0;

        /**
        * @brief Simulation time the job was queued at.
        */
        TickType mQueuedAt = 0;

        /**
        * @brief Simulation time the job was most recently updated.
        */
        TickType mLastTime = 0;

        /**
        * @brief Size of the already downloaded job input data
        */
        SpaceType mCurInputFileSize = 0;

        /**
        * @brief Pointer to the file that is required as input by the job
        */
        SFile* mInputFile = nullptr;

        /**
        * @brief The chosen replica to download
        */
        SReplica* mInputReplica = nullptr;

        /**
        * @brief The new replicas created as output of the job
        */
        std::vector<SReplica*> mOutputReplicas;
    };

    /**
    * @brief Type that represents a list of job info objects
    */
    typedef std::list<std::unique_ptr<SJobInfo>> JobInfoList;


    // configuration data / initialised by config

    /**
    * @brief the storage element representing the hot storage
    */
    CStorageElement* mHotStorageElement = nullptr;

    /**
    * @brief the storage element representing the cold storage
    */
    CStorageElement* mColdStorageElement = nullptr;

    /**
    * @brief the storage element representing the archival storage
    */
    CStorageElement* mArchiveStorageElement = nullptr;


    /**
    * @brief caching a pointer of the network link from the archival to the hot storage
    */
    CNetworkLink* mArchiveToHotLink = nullptr;

    /**
    * @brief caching a pointer of the network link from the archival to the cold storage
    */
    CNetworkLink* mArchiveToColdLink = nullptr;

    /**
    * @brief caching a pointer of the network link from the archival to the local worker node
    */
    CNetworkLink* mHotToCPULink = nullptr;

    /**
    * @brief caching a pointer of the network link from the local worker node to the output storage element
    */
    CNetworkLink* mCPUToOutputLink = nullptr;

    /**
    * @brief simulation time when the production workflow started
    */
    TickType mProductionStartTime = 0;

    /**
    * @brief limit of available job slots. This could be used to implement a job slot based model rather than a random distribution based.
    */
    std::size_t mNumCores = 0;

    /**
    * @brief value generator describing the probability of how often data will be reused
    */
    std::unique_ptr<IValueGenerator> mReusageNumGen;

    /**
    * @brief value generator describing the number of jobs that will be submitted
    */
    std::unique_ptr<IValueGenerator> mNumJobSubmissionGen;

    /**
    * @brief value generator describing the distribution of the job duration
    */
    std::unique_ptr<IValueGenerator> mJobDurationGen;

    /**
    * @brief value generator describing the number of output replicas to generate
    */
    std::unique_ptr<IValueGenerator> mNumOutputGen;

    /**
    * @brief value generator describing the size of the output replicas
    */
    std::unique_ptr<IValueGenerator> mOutputSizeGen;


    // runtime data / initialised automatically / book keeping
    /**
    * @brief groups files by popularity
    */
    std::vector<std::vector<SFile*>> mArchiveFilesPerPopularity;

    /**
    * @brief maps a popularity value to a SIndexedReplicas instance
    */
    std::map<std::uint32_t, SIndexedReplicas> mHotReplicasByPopularity;

    /**
    * @brief maps a popularity value to a list of replicas available at the cold storage
    */
    std::map<std::uint32_t, std::forward_list<SReplica*>>  mColdReplicasByPopularity;

    
    /**
    * @brief hot replicas that will be deleted after their transfer to cold storage is done
    */
    std::unordered_set<SReplica*> mHotReplicaDeletions;

    /**
    * @brief replicas that are queued for deletion. This allows simulating a delay in the deletion rather than deleting everything instantly.
    */
    std::map<TickType, std::vector<SReplica*>> mHotReplicasDeletionQueue;

    /**
    * @brief jobs in the waiting state
    */
    JobInfoList mWaitingJobs;

    /**
    * @brief jobs in the transferring state
    */
    std::unordered_map <SReplica*, JobInfoList> mTransferringJobs;

    /**
    * @brief jobs in the queued state
    */
    JobInfoList mQueuedJobs;

    /**
    * @brief newly created jobs
    */
    JobInfoList mNewJobs;

    /**
    * @brief jobs that are currently downloading
    */
    JobInfoList mDownloadingJobs;

    /**
    * @brief jobs in the running state. Ordered by their finishing time.
    */
    std::map<TickType, JobInfoList> mRunningJobs;

    /**
    * @brief jobs that are currently uploading
    */
    JobInfoList mUploadingJobs;

    /**
    * @brief this hash table maps a file object that is waiting for a certain file to an array of jobs that are waiting for the same file.
    */
    std::unordered_map <SFile*, std::vector<JobInfoList::iterator>> mWaitingForSameFile;

    /**
    * @brief number of jobs currently active
    */
    std::size_t mNumJobs = 0;

    /**
    * @brief stores decimal places for the job submission value generator in case the schedule frequency and random parameters are not chosen properly
    */
    double mNumJobSubmissionAccu = 0;

private:
    /**
    * @brief Internal helper function to get a random distribubtion based on the existing popularity values
    */
    std::discrete_distribution<std::size_t> GetPopularityIdxRNG();

    /**
    * @brief Queues a replica at hot storage for deletion
    * 
    * @param replica a valid pointer to a replica at hot storage that should be removed
    * @param expireAt artificial delay to apply to the deletion
    * 
    * The deletion will try to transfer the replica to the cold storage prior to its deletion. If cold storage
    * is full the deletion will potentially be delayed further.
    */
    void QueueHotReplicasDeletion(SReplica* replica, TickType expireAt);

    /**
    * @brief Deletes all expired replicas at hot storage if they can be transferred to cold storage
    * 
    * @param now current simulation time
    * 
    * @return the amount of cold storage required to delete further hot storage replicas
    */
    SpaceType DeleteQueuedHotReplicas(TickType now);

    /**
    * @brief Update the derivation production workflow as described by the HCDC model
    *
    * @param now current simulation time
    */
    void UpdateProductionCampaign(TickType now);

    /**
    * @brief Update deletions of hot storage replicas that are pending, e.g., waiting for a transfer to cold storage to complete.
    *
    * @param now current simulation time
    */
    void UpdatePendingDeletions(TickType now);

    /**
    * @brief Update jobs in waiting state and creates transfer for their input data if possible
    *
    * @param now current simulation time
    */
    void UpdateWaitingJobs(TickType now);

    /**
    * @brief Updates all active jobs downloading their input data, executing the payload, and uploading their output data.
    *
    * @param now current simulation time
    */
    void UpdateActiveJobs(TickType now);

    /**
    * @brief Updates all queued jobs activating them if requirements are fulfilled.
    *
    * @param now current simulation time
    */
    void UpdateQueuedJobs(TickType now);

    /**
    * @brief Creates new job objects, randomly selecting input data.
    *
    * @param now current simulation time
    */
    void SubmitNewJobs(TickType now);


    /**
    * @brief Runs a preparation phase for the production workflow during which data can be pre transferred to from
    * archival storage to cold storage based on the popularity.
    *
    * @param now current simulation time
    */
    void PrepareProductionCampaign(TickType now);

public:
    /**
    * @brief action interface implementations that allow keeping all member variables in sync,
    *        e.g., a hot storage replica that was completed will queue all jobs that were waiting for this replica
    *
    * @param replica pointer to the replica that was completed
    * @param now current simulation time
    */
    void PostCompleteReplica(SReplica* replica, TickType now) override;

    /**
    * @brief action interface implementations that allow keeping all member variables in sync,
    *        e.g., adds a new hot storage replica to the mHotReplicasByPopularity map
    *
    * @param replica pointer to the replica that was created
    * @param now current simulation time
    */
    void PostCreateReplica(SReplica* replica, TickType now) override;

    /**
    * @brief action interface implementations that allow keeping all member variables in sync,
    *        e.g., removes a hot replica from the mHotReplicasByPopularity map
    *
    * @param replica pointer to the replica that will be removed
    * @param now current simulation time
    */
    void PreRemoveReplica(SReplica* replica, TickType now) override;

    /**
    * @brief Constructs the HCDC transfer generator
    * 
    * @param sim valid pointer to the associated simulation engine
    * @param transferMgr valid shared pointer to the transfer manager instance this generator will use
    * @param tickFreq the frequency the generator should be ticked at
    * @param startTick the first time the generator should be executed
    */
    CHCDCTransferGen(IBaseSim* sim,
        std::shared_ptr<CTransferManager> transferMgr,
        TickType tickFreq,
        TickType startTick = 0);

    /**
    * @brief Called by the event loop to execute the payload. Calls the PrepareProductionCampaign() and UpdateProductionCampaign() methods
    *
    * @param now current simulation time
    */
    void OnUpdate(TickType now) final;

    /**
    * @brief Method called by the simulation engine to notify the event that the simulation is shutting down
    *
    * @param now current simulation time
    */
    void Shutdown(const TickType now) final;
};



/**
* @brief Creates transfers between two storage elements and uses a cloud storage element to buffer data if the primary destination is fully occupied.
*
* Create transfers from a configured source to a configured primary destination. If the storage space of the primary destination becomes full, data
* are transferred to a secondary buffer destination. The number of generated transfers depends on the available replicas and the network link limits.
*/
class CCloudBufferTransferGen : public CSchedulable, public IStorageElementActionListener
{
private:
    /**
    * @brief pointer to the associated simulation engine
    */
    IBaseSim* mSim;

    /**
    * @brief pointer to the object managing the transfers of this generator
    */
    std::shared_ptr<CTransferManager> mTransferMgr;

    /**
    * @brief the frequency in which this event should be called
    */
    TickType mTickFreq;

public:

    /**
    * @brief Helper struct to describe the transfer generation details.
    * 
    * Add objects of this struct to the mTransferGenInfo array to allow the generator to generate transfers.
    */
    struct STransferGenInfo
    {
        /**
        * @brief value generator used to specify how often a replica might be reconsidered for transfer
        */
        std::unique_ptr<IValueGenerator> mReusageNumGen;

        /**
        * @brief the network link from the source to the primary storage element, e.g., disk
        */
        CNetworkLink* mPrimaryLink;

        /**
        * @brief the network link from the source to the secondary storage element, e.g., cloud
        */
        CNetworkLink* mSecondaryLink;

        /**
        * @brief list of source replicas to transfer
        */
        std::forward_list<SReplica*> mReplicas;
    };

    /**
    * @brief contains the generation configuration in form of STransferGenInfo objects
    */
    std::vector<std::unique_ptr<STransferGenInfo>> mTransferGenInfo;

    /**
    * @brief if true, deletes the source replicas
    */
    bool mDeleteSrcReplica = false;

    /**
    * @brief action interface implementation. Initialisese the source file popularity
    *
    * @param replica pointer to the replica that was completed
    * @param now current simulation time
    */
    void PostCompleteReplica(SReplica* replica, TickType now) override;

    /**
    * @brief action interface implementation. Does nothing.
    *
    * @param replica pointer to the replica that was created
    * @param now current simulation time
    */
    void PostCreateReplica(SReplica* replica, TickType now) override;

    /**
    * @brief action interface implementation. Removes a source replica if it gets removed.
    *
    * @param replica pointer to the replica that will be removed
    * @param now current simulation time
    * 
    * Is not expected to be invoked durin runtime.
    */
    void PreRemoveReplica(SReplica* replica, TickType now) override;

public:
    /**
    * @brief Constructs the transfer generator
    *
    * @param sim valid pointer to the associated simulation engine
    * @param transferMgr valid shared pointer to the transfer manager instance this generator will use
    * @param tickFreq the frequency the generator should be ticked at
    * @param startTick the first time the generator should be executed
    */
    CCloudBufferTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        const TickType tickFreq,
                        const TickType startTick=0 );
    ~CCloudBufferTransferGen();

    /**
    * @brief Called by the event loop to execute the payload.
    *
    * @param now current simulation time
    */
    void OnUpdate(const TickType now) final;
};



/**
* @brief (Work in progress) transfer generator based on job slots
* 
* The transfer generator uses job slots to deteremine how many jobs/transfers can be created.
*/
class CJobSlotTransferGen : public CSchedulable
{
private:
    /**
    * @brief pointer to the associated simulation engine
    */
    IBaseSim* mSim;

    /**
    * @brief pointer to the object managing the transfers of this generator
    */
    std::shared_ptr<CFixedTimeTransferManager> mTransferMgr;

    /**
    * @brief the frequency in which this event should be called
    */
    TickType mTickFreq;

public:

    /**
    * @brief Helper struct to configure this transfer generator
    */
    struct SJobSlotInfo
    {
        /**
        * @brief maximum number of usable slots
        */
        std::uint32_t mNumMaxSlots;

        /**
        * @brief pairs of job finishing times and occupied job slots
        */
        std::vector<std::pair<TickType, std::uint32_t>> mSchedule;
    };


    /**
    * @brief maps a source storage element id to a job priority
    */
    std::unordered_map<IdType, int> mSrcStorageElementIdToPrio;

    /**
    * @brief maps a destination storage element to the job slot info
    */
    std::vector<std::pair<CStorageElement*, SJobSlotInfo>> mDstInfo;

public:
    /**
    * @brief Constructs the transfer generator
    *
    * @param sim valid pointer to the associated simulation engine
    * @param transferMgr valid shared pointer to the transfer manager instance this generator will use
    * @param tickFreq the frequency the generator should be ticked at
    * @param startTick the first time the generator should be executed
    */
    CJobSlotTransferGen(IBaseSim* sim,
                        std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                        TickType tickFreq,
                        TickType startTick=0 );

    /**
    * @brief Called by the event loop to execute the payload.
    *
    * @param now current simulation time
    */
    void OnUpdate(TickType now) final;
};



/**
* @brief Transfer generator that contains storage elements that serve as cache for source replicas.
* 
* Generates transfers based on the fixed mNumPerDay value. The main part is the selection of the best
* source replica. If the replica can not be transferred from a cache it will be additionally transferred
* there. The files are removed from the cache in dependence of how likely it is that they are used again.
* Assuming each subsequent access will make another access more unlikely.
*/
class CCachedSrcTransferGen : public CSchedulable
{
private:
    /**
    * @brief pointer to the associated simulation engine
    */
    IBaseSim* mSim;

    /**
    * @brief pointer to the object managing the transfers of this generator
    */
    std::shared_ptr<CFixedTimeTransferManager> mTransferMgr;

    /**
    * @brief the frequency in which this event should be called
    */
    TickType mTickFreq;

    /**
    * @brief Checks if a given file has a replica at a given storage element
    * 
    * @param file the file object in question
    * @param storageElement the storage element to search on
    * 
    * @return true if the file has a replica at the given storage element, false otherwise
    */
    bool ExistsFileAtStorageElement(const SFile* file, const CStorageElement* storageElement) const;


    /**
    * @brief Deletes all expired replicas at the given storage element
    *
    * @param storageElement the storage element to cleanup
    * @param now the current simulation time
    */
    void ExpireReplica(CStorageElement* storageElement, TickType now);

public:
    /**
    * @brief Helper struct to describe a cached storage element
    */
    struct SCacheElementInfo
    {
        std::size_t mCacheSize;
        TickType mDefaultReplicaLifetime;
        CStorageElement* mStorageElement;
    };


    /**
    * @brief Constructs the transfer generator
    *
    * @param sim valid pointer to the associated simulation engine
    * @param transferMgr valid shared pointer to the transfer manager instance this generator will use
    * @param numPerDay number of transfers to generate per day
    * @param defaultReplicaLifetime the default life time of newly created replicas
    * @param tickFreq the frequency the generator should be ticked at
    * @param startTick the first time the generator should be executed
    */
    CCachedSrcTransferGen(IBaseSim* sim,
                        std::shared_ptr<CFixedTimeTransferManager> transferMgr,
                        std::size_t numPerDay,
                        TickType defaultReplicaLifetime,
                        TickType tickFreq,
                        TickType startTick=0 );

    /**
    * @brief Contains pairs of a ratio and file objects per access count.
    * 
    * The first element contains files that have been accessed only once. The second element
    * conotains files that have been accessed twice. And so on. The ratio describes the share
    * of files that will be used from this element. In other words, the more often a file is
    * used, the less likely it becomes to be used again. However, the variable can be customies
    * by the user.
    */
    std::vector<std::pair<float, std::vector<SFile*>>> mRatiosAndFilesPerAccessCount{ {0.62f, {}}, {0.16f, {}}, {0.08f, {}}, {0.05f, {}} };

    /**
    * @brief storage elements containing the source replicas
    */
    std::vector<CStorageElement*> mSrcStorageElements;

    /**
    * @brief storage elements serving as cache
    */
    std::vector<SCacheElementInfo> mCacheElements;

    /**
    * @brief destination storage elements
    */
    std::vector<CStorageElement*> mDstStorageElements;

    /**
    * @brief number of transfers to generate per simulated day
    */
    std::size_t mNumPerDay;

    /**
    * @brief default life time of newly created replicas
    */
    TickType mDefaultReplicaLifetime;

    /**
    * @brief Called by the event loop to execute the payload.
    *
    * @param now current simulation time
    */
    void OnUpdate(TickType now) final;
};



/**
* @brief Simple transfer generator that generates transfers based on a given value generator
*/
class CFixedTransferGen : public CSchedulable, public IStorageElementActionListener
{
private:
    /**
    * @brief pointer to the associated simulation engine
    */
    IBaseSim* mSim;

    /**
    * @brief pointer to the object managing the transfers of this generator
    */
    std::shared_ptr<CTransferManager> mTransferMgr;

    /**
    * @brief the frequency in which this event should be called
    */
    TickType mTickFreq;

    /**
    * @brief replicas whose transfer were finished and notified by the action interface
    */
    std::vector<SReplica*> mCompleteReplicas;
    
public:
    /**
    * @brief Constructs the transfer generator
    * 
    * @param sim valid pointer to the associated simulation engine
    * @param transferMgr valid shared pointer to the transfer manager instance this generator will use
    * @param tickFreq the frequency the generator should be ticked at
    * @param startTick the first time the generator should be executed
    */
    CFixedTransferGen(IBaseSim* sim,
                        std::shared_ptr<CTransferManager> transferMgr,
                        TickType tickFreq,
                        TickType startTick=0 );

    /**
    * @brief helper struct to map a value generator with a destination storage element
    */
    struct STransferGenInfo
    {
        /**
        * @brief destination storage element
        */
        CStorageElement* mDstStorageElement = nullptr;

        /**
        * @brief value generator describing the number of transfer to generate per update
        */
        std::unique_ptr<IValueGenerator> mNumTransferGen;

        /**
        * @brief accumulator counting the lost decimal places in case of high update frequencies
        */
        double mDecimalAccu = 0;
    };

    /**
    * @brief array with pairs of source storage elements and STransferGenInfo objects
    * 
    * The transfer generator will pick random replicas from the source storage element based on
    * the value generator and transfer them to the corresponding destination storage element.
    */
    std::vector<std::pair<CStorageElement*, std::vector<STransferGenInfo>>> mConfig;

    /**
    * @brief action interface implementations. Stores the completed replica to be deleted again next update.
    *
    * @param replica pointer to the replica that was completed
    * @param now current simulation time
    */
    void PostCompleteReplica(SReplica* replica, TickType now) override;

    /**
    * @brief action interface implementation. Does nothing.
    *
    * @param replica pointer to the replica that was created
    * @param now current simulation time
    */
    void PostCreateReplica(SReplica* replica, TickType now) override;

    /**
    * @brief action interface implementation. Does nothing.
    *
    * @param replica pointer to the replica that will be removed
    * @param now current simulation time
    */
    void PreRemoveReplica(SReplica* replica, TickType now) override;

    /**
    * @brief Called by the event loop to execute the payload.
    *
    * @param now current simulation time
    */
    void OnUpdate(TickType now) final;

    /**
    * @brief Method called by the simulation engine to notify the event that the simulation is shutting down
    *
    * @param now current simulation time
    */
    void Shutdown(const TickType now) final;
};
