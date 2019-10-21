#pragma once

#include "CDefaultBaseSim.hpp"

class CTestSim : public CDefaultBaseSim
{
public:
    bool SetupDefaults(const json& profileJson) override;
};
