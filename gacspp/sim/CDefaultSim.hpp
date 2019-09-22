#pragma once

#include "IBaseSim.hpp"

class CDefaultSim : public IBaseSim
{
public:
    void SetupDefaults(const json& profileJson) override;
};
