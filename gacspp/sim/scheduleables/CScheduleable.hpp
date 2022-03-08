/**
 * @file   CScheduleable.hpp
 * @brief  Contains the base class for every schedulable object
 *
 * @author Tobias Wegner
 * @date   March 2022
 */

#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "common/constants.h"
#include <set>

/**
* @brief Basic class for every schedulable event
*/
class CSchedulable
{
public:
    /**
    * @brief name of the event
    */
    std::string mName;

    /**
    * @brief The real time duration that this event spent in the OnUpdate() method
    */
    DurationType mUpdateDurationSummed = DurationType::zero();

    /**
    * @brief Various string duration pairs used for debugging purposes
    */
    std::vector<std::pair<std::string, DurationType>> mDebugDurations;

    /**
    * @brief schedule time point of this event
    */
    TickType mNextCallTick;

    /**
    * @brief Constructs this object
    * 
    * @param startTick the first simulation time point this event will be called at
    */
    CSchedulable(const TickType startTick=0)
        : mNextCallTick(startTick)
    {}

    virtual ~CSchedulable() = default;

    /**
    * @brief Executes the payload of this schedulable
    * 
    * @param now current simulation time
    */
    virtual void OnUpdate(const TickType now) = 0;

    /**
    * @brief Notifies the schedulable that the simulation is shutting down
    * 
    * @param now current simulation time
    */
    virtual void Shutdown(const TickType now){(void)now;};
};

/**
* @brief This struct represents a functor to allow comparison of pointers to CSchedulable objects
*/
struct SSchedulePrioComparer
{
    bool operator()(const CSchedulable* left, const CSchedulable* right) const;
    bool operator()(const std::shared_ptr<CSchedulable>& left, const std::shared_ptr<CSchedulable>& right) const;
};

//typedef std::priority_queue<std::shared_ptr<CScheduleable>, std::vector<std::shared_ptr<CScheduleable>>, SSchedulePrioComparer> ScheduleType;


/**
* @brief ScheduleType provides a type for the schedule which is implemented using a multiset
*/
typedef std::multiset<std::shared_ptr<CSchedulable>, SSchedulePrioComparer> ScheduleType;
