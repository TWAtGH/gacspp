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


class IBaseSim
{
public:
    IBaseSim();
    IBaseSim(const IBaseSim&) = delete;
    IBaseSim& operator=(const IBaseSim&) = delete;
    IBaseSim(const IBaseSim&&) = delete;
    IBaseSim& operator=(const IBaseSim&&) = delete;

    virtual ~IBaseSim();

    RNGEngineType mRNGEngine;

    //rucio and clouds
    std::unique_ptr<CRucio> mRucio;
    std::vector<std::unique_ptr<IBaseCloud>> mClouds;

    virtual bool SetupDefaults(const json& profileJson) = 0;
    virtual void Run(TickType maxTick);

    void Stop();

public:
    auto GetStorageElementByName(const std::string& name) const -> CStorageElement*;

protected:
    bool mIsRunning = false;
    ScheduleType mSchedule;

private:
    TickType mCurrentTick;
};
