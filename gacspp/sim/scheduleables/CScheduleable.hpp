#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "common/constants.h"

class CScheduleable
{
public:
    std::string mName;
    DurationType mUpdateDurationSummed = DurationType::zero();
    TickType mNextCallTick;

    CScheduleable(const TickType startTick=0)
        : mNextCallTick(startTick)
    {}

    virtual ~CScheduleable() = default;
    virtual void OnUpdate(const TickType now) = 0;
    virtual void Shutdown(const TickType now){(void)now;};
};

struct SSchedulePrioComparer
{
    bool operator()(const CScheduleable *left, const CScheduleable *right) const;
    bool operator()(const std::shared_ptr<CScheduleable>& left, const std::shared_ptr<CScheduleable>& right) const;
};

typedef std::priority_queue<std::shared_ptr<CScheduleable>, std::vector<std::shared_ptr<CScheduleable>>, SSchedulePrioComparer> ScheduleType;
