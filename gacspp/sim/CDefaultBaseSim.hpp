#pragma once

#include "IBaseSim.hpp"


class CBaseTransferManager;

class CDefaultBaseSim : public IBaseSim
{
public:
    bool SetupDefaults(const json& profileJson) override;

    virtual bool SetupRucio(const json& profileJson);
    virtual bool SetupClouds(const json& profileJson);

    virtual bool AddGridToOutput();
    virtual bool AddCloudsToOutput();

    virtual bool SetupLinks(const json& profileJson);

    virtual auto CreateTransferManager(const json& transferManagerCfg) const -> std::shared_ptr<CBaseTransferManager>;
    virtual auto CreateTransferGenerator(const json& transferGenCfg, const std::shared_ptr<CBaseTransferManager>& transferManager) -> std::shared_ptr<CScheduleable>;
};
