#pragma once

#include "CDefaultBaseSim.hpp"

class CDeterministicSim01 : public CDefaultBaseSim
{
public:
    bool SetupDefaults(const json& profileJson) override;
};
