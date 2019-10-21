#include <iostream>

#include "CDeterministicSim01.hpp"

#include "CommonScheduleables.hpp"

#include "third_party/json.hpp"



bool CDeterministicSim01::SetupDefaults(const json& profileJson)
{
    if(!CDefaultBaseSim::SetupDefaults(profileJson))
        return false;

    return true;
}
