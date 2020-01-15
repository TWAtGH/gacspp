#pragma once

#include "CDefaultBaseSim.hpp"

class CBufferedOnDeletionInsert;

class CTestSim : public CDefaultBaseSim
{
private:
    std::shared_ptr<CBufferedOnDeletionInsert> mDeletionInserter;

public:
    bool SetupDefaults(const json& profileJson) override;
};
