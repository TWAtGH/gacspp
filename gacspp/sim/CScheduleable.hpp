#pragma once

#include <chrono>
#include <memory>
#include <queue>
#include <vector>

#include "constants.h"

class CScheduleable
{
public:
    std::chrono::duration<double> mUpdateDurationSummed = std::chrono::duration<double>::zero();
    TickType mNextCallTick;

    CScheduleable(const TickType startTick=0)
        : mNextCallTick(startTick)
    {}

    virtual ~CScheduleable() = default;
    virtual void OnUpdate(const TickType now) = 0;
};

struct SSchedulePrioComparer
{
    bool operator()(const CScheduleable *left, const CScheduleable *right) const;
    bool operator()(const std::shared_ptr<CScheduleable>& left, const std::shared_ptr<CScheduleable>& right) const;
};

typedef std::priority_queue<std::shared_ptr<CScheduleable>, std::vector<std::shared_ptr<CScheduleable>>, SSchedulePrioComparer> ScheduleType;
