#pragma once

#include "third_party/nlohmann/json_fwd.hpp"

using nlohmann::json;


class IConfigConsumer
{
public:
    virtual bool LoadConfig(const json& config) = 0;
};
