#pragma once

#include <cstdint>
#include <vector>
#include <queue>


class CScheduleable;

struct SSchedulePrioComparer
{
    bool operator()(const CScheduleable *left, const CScheduleable *right) const;
};

typedef std::priority_queue<CScheduleable*, std::vector<CScheduleable*>, SSchedulePrioComparer> ScheduleType;

class CScheduleable
{
public:
    std::uint64_t mNextCallTick = 0;
    virtual ~CScheduleable() = default;
    virtual void OnUpdate(ScheduleType &schedule, std::uint64_t now) = 0;
};
