#pragma once

#include <memory>
#include <random>
#include <vector>

#include "CScheduleable.hpp"

#include "common/constants.h"

#include "third_party/json_fwd.hpp"



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
