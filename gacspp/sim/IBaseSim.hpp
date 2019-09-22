#pragma once

#include <memory>
#include <random>
#include <vector>

#include "json_fwd.hpp"

#include "constants.h"

#include "CScheduleable.hpp"


using nlohmann::json;

class IBaseCloud;
class CRucio;


class IBaseSim
{
public:
    IBaseSim();
    virtual ~IBaseSim();

    //std::random_device rngDevice;
    RNGEngineType mRNGEngine {42};

    //rucio and clouds
    std::unique_ptr<CRucio> mRucio;
    std::vector<std::unique_ptr<IBaseCloud>> mClouds;

    virtual void SetupDefaults(const json& profileJson) = 0;
    virtual void Run(const TickType maxTick);

protected:
    ScheduleType mSchedule;

private:
    TickType mCurrentTick;
};
