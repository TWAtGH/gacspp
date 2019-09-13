#pragma once

#include "json_fwd.hpp"

using nlohmann::json;


class IConfigConsumer
{
public:
    virtual bool TryConsumeConfig(const json& config) = 0;
};
