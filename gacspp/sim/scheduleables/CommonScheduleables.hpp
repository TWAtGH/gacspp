/**
 * @file   CommonScheduleables.hpp
 * @brief  Contains classes for common schedulables
 *
 * @author Tobias Wegner
 * @date   March 2022
 */

#pragma once

#include "CScheduleable.hpp"

#include "common/utils.hpp"

class IPreparedInsert;
class IBaseSim;
class CRucio;
class CBaseTransferManager;
class CStorageElement;


/**
* @brief Event that can be used to generate data frequently or initially
*/
class CDataGenerator : public CSchedulable
{
private:

    /**
    * @brief pointer to the associated simulation engine
    */
    IBaseSim* mSim;

    /**
    * @brief value generator describing the number of files to generate
    */
    std::unique_ptr<IValueGenerator> mNumFilesGen;

    /**
    * @brief value generator to generate the file size for each file
    */
    std::unique_ptr<IValueGenerator> mFileSizeGen;

    /**
    * @brief value generator to generate the life time of each file
    */
    std::unique_ptr<IValueGenerator> mFileLifetimeGen;

    /**
    * @brief the frequency in which this event should be called. 0 means just once
    */
    TickType mTickFreq;


    /**
    * @brief Internal function to create the given a given number of files and a given number of replicas per file
    * 
    * @param numFiles number of files to generate
    * @param numReplicasPerFile number of replicas to generate per file
    * @param now current simulation time
    */
    void CreateFilesAndReplicas(const std::uint32_t numFiles, const std::uint32_t numReplicasPerFile, const TickType now);

public:

    /**
    * @brief if true storage elements are selected randomly, otherwise they are accessed in order
    */
    bool mSelectStorageElementsRandomly = false;

    /**
    * @brief Ratios of the number of files to create multiple replicas for
    * 
    * If the array is empty only on replica will be created for each file. Otherwise, the ratios in the array
    * are used to create multiple replicas for a corresponding share of the total number of files to generate.
    */
    std::vector<float> mNumReplicaRatio;

    /**
    * @brief storage elements to consider for replica creation
    */
    std::vector<CStorageElement*> mStorageElements;

    /**
    * @brief Constructs the data generator
    * 
    * @param sim the associated simulation engine
    * @param numFilesRNG the value generator for the number of files
    * @param fileSizeRNG the value generator for the file size
    * @param fileLifetimeRNG the value generator for the file life time
    * @param tickFreq the frequency the data generator should be ticked at. 0 means just once
    * @param startTick the first time the data generator should tick
    */
    CDataGenerator( IBaseSim* sim,
                    std::unique_ptr<IValueGenerator>&& numFilesRNG,
                    std::unique_ptr<IValueGenerator>&& fileSizeRNG,
                    std::unique_ptr<IValueGenerator>&& fileLifetimeRNG,
                    const TickType tickFreq,
                    const TickType startTick=0);

    /**
    * @brief Creates files and replicas. Usese the member variables to call the private method CreateFilesAndReplicas()
    */
    void CreateFilesAndReplicas(const TickType now);

    /**
    * @brief Called by the event loop to execute the payload. Calls CreateFilesAndReplicas()
    * 
    * @param now current simulation time
    */
    void OnUpdate(const TickType now) final;
};



/**
* @brief Event that regularly uses CRucio to remove expired files and replicas
*/
class CReaperCaller : public CSchedulable
{
private:
    /**
    * @brief associated CRucio instance
    */
    CRucio *mRucio;

    /**
    * @brief the frequency in which this event should be called. 0 means just once
    */
    TickType mTickFreq;

public:
    /**
    * @brief Constructs the object
    * 
    * @param rucio a valid pointer to the associated CRucio instance
    * @param tickFreq the frequency the reaper should be called at
    * @param startTick the first time the reapepr should be called
    */
    CReaperCaller(CRucio *rucio, const TickType tickFreq, const TickType startTick=600);

    /**
    * @brief Called by the event loop to execute the payload. Calls the reaper registered at CRucio
    * 
    * @param now current simulation time
    */
    void OnUpdate(const TickType now) final;
};



/**
* @brief Event that regularly triggers the cost calculation of all existing cloud instances
*/
class CBillingGenerator : public CSchedulable
{
private:
    /**
    * @brief Query used to write the bills to the output system
    */
    std::shared_ptr<IPreparedInsert> mCloudBillInsertQuery;

    /**
    * @brief pointer to the associated simulation engine
    */
    IBaseSim* mSim;

    /**
    * @brief the frequency in which this event should be called
    */
    TickType mTickFreq;

public:
    /**
    * @brief Constructs the billing generator
    *
    * @param sim valid pointer to the associated simulation engine
    * @param tickFreq the frequency the billing generator should be ticked at
    * @param startTick the first time the billing generator should be executed
    */
    CBillingGenerator(IBaseSim* sim, const TickType tickFreq = SECONDS_PER_MONTH, const TickType startTick = SECONDS_PER_MONTH);

    /**
    * @brief Called by the event loop to execute the payload. Calls the billing processing functions of all IBaseCloud instances
    * 
    * @param now current simulation time
    */
    void OnUpdate(const TickType now) final;
};



/**
* @brief Event that regularly prints several statistics to the standard output
*/
class CHeartbeat : public CSchedulable
{
private:
    /**
    * @brief pointer to the associated simulation engine
    */
    IBaseSim* mSim;

    /**
    * @brief the frequency in which this event should be called
    */
    TickType mTickFreq;

    /**
    * @brief last time the event was called
    */
    std::chrono::high_resolution_clock::time_point mTimeLastUpdate;

public:
    /**
    * @brief real time durations of registered schedulables
    */
    std::vector<std::weak_ptr<CSchedulable>> mProccessDurations;

    /**
    * @brief registered transfer managers to print transfer statistics
    */
    std::vector<std::shared_ptr<CBaseTransferManager>> mTransferManagers;

public:
    /**
    * @brief Constructs the heartbeat event
    *
    * @param sim valid pointer to the associated simulation engine
    * @param tickFreq the frequency the heartbeat should be ticked at
    * @param startTick the first time the heartbeat should be executed
    */
    CHeartbeat(IBaseSim* sim, const TickType tickFreq, const TickType startTick = 0);

    /**
    * @brief Called by the event loop to execute the payload. Uses the member variables to print statistics.
    * 
    * @param now current simulation time
    */
    void OnUpdate(const TickType now) final;
};
