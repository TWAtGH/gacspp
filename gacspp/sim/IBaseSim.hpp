/**
 * @file   IBaseSim.hpp
 * @brief  Contains the basic interface definition of the simulation engine.
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * The IBaseSim class defines the interface for a simulation engine. Every simulation
 * engine must contain a Rucio instance for managing simulated grid resources. In addition,
 * access to all potential cloud instances is provided.
 * The base simulation engine implements a straightforward event loop in the Run() method.
 * However, the SetupDefaults() function must be implemented to setup the fundamental infrastructure.
 *
 */
#pragma once

#include <memory>
#include <random>
#include <vector>

#include "scheduleables/CScheduleable.hpp"

#include "common/constants.h"

#include "third_party/nlohmann/json_fwd.hpp"



using nlohmann::json;

class IBaseCloud;
class CRucio;
class CStorageElement;


/**
* @brief Base interface for a simulation engine
*
* SetupDefaults() must be implemented to create the Rucio object and potential cloud objects and apply their configurations.
* Calling Run() will start the simulation until the given max tick is reached, Stop() is called, or the simulation converged
* to an end.
*/
class IBaseSim
{
public:
    IBaseSim();
    IBaseSim(const IBaseSim&) = delete;
    IBaseSim& operator=(const IBaseSim&) = delete;
    IBaseSim(const IBaseSim&&) = delete;
    IBaseSim& operator=(const IBaseSim&&) = delete;

    virtual ~IBaseSim();

    /**
    * @brief Stores the random number generation engine
    */
    RNGEngineType mRNGEngine;

    /**
    * @brief Stores the only instance of the CRucio class providing access to simulated grid resources
    */
    std::unique_ptr<CRucio> mRucio;

    /**
    * @brief Stores the only instances of potential IBaseCloud implementations
    */
    std::vector<std::unique_ptr<IBaseCloud>> mClouds;


    /**
    * @brief Initialises the simulated infrastructure. Must be called prior to Run()
    * 
    * @param profileJson json object containing configuration data for the simulation engine
    * 
    * @return true on successfull initialisation or false otherwise
    */
    virtual bool SetupDefaults(const json& profileJson) = 0;


    /**
    * @brief Runs the simulation until a stop criterion is reached
    * 
    * @param maxTick maximum simulation time before the simulation will finish
    */
    virtual void Run(TickType maxTick);


    /**
    * @brief Stops the event loop and finishes the simulation
    */
    void Stop();

public:

    /**
    * @brief Helper function to find a storage element by name
    * 
    * @param name name of the desired storage element
    * 
    * @return A pointer to the storage element if found or nullptr otherwise.
    * 
    * This method first iterates through all grid sites and searches the storage element.
    * If it was not found, all cloud regions will be searched as well.
    */
    auto GetStorageElementByName(const std::string& name) const -> CStorageElement*;


    /**
    * @brief Provides access to the currently running simulation engine instance
    */
    static IBaseSim* Sim;

protected:

    /**
    * @brief indicates if the simulation is running
    */
    bool mIsRunning = false;


    /**
    * @brief the schedule containing the events to process in the event loop
    */
    ScheduleType mSchedule;

private:

    /**
    * @brief The simulation clock representing the current simulation time. 
    */
    TickType mCurrentTick;
};
