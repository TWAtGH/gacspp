#pragma once

#include "IBaseSim.hpp"

class CDefaultBaseSim : public IBaseSim
{
public:
    bool SetupDefaults(const json& profileJson) override;

    virtual bool SetupRucio(const json& profileJson);
    virtual bool SetupClouds(const json& profileJson);

    virtual bool AddGridToOutput();
    virtual bool AddCloudsToOutput();

    virtual bool SetupLinks(const json& profileJson);
};
