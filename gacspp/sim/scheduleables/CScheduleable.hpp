#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "common/constants.h"
#include <set>

class CSchedulable
{
public:
    std::string mName;
    DurationType mUpdateDurationSummed = DurationType::zero();
    std::vector<std::pair<std::string, DurationType>> mDebugDurations;
    TickType mNextCallTick;

    CSchedulable(const TickType startTick=0)
        : mNextCallTick(startTick)
    {}

    virtual ~CSchedulable() = default;
    virtual void OnUpdate(const TickType now) = 0;
    virtual void Shutdown(const TickType now){(void)now;};
};

struct SSchedulePrioComparer
{
    bool operator()(const CSchedulable* left, const CSchedulable* right) const;
    bool operator()(const std::shared_ptr<CSchedulable>& left, const std::shared_ptr<CSchedulable>& right) const;
};

//typedef std::priority_queue<std::shared_ptr<CScheduleable>, std::vector<std::shared_ptr<CScheduleable>>, SSchedulePrioComparer> ScheduleType;
typedef std::multiset<std::shared_ptr<CSchedulable>, SSchedulePrioComparer> ScheduleType;
