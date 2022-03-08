/**
 * @file   IConfigConsumer.hpp
 * @brief  Provides a basic interface for every class that is interested in receiving configuration data.
 *
 * @author Tobias Wegner
 * @date   March 2022
 */

#pragma once

#include "third_party/nlohmann/json_fwd.hpp"

using nlohmann::json;

/**
 * @brief Must be implemented by any class that is interested in receiving configuration data.
 */
class IConfigConsumer
{
public:
    /**
     * @brief Called by the simulation engine to apply the given configuration data.
     * 
     * @param config the configuration data in json format
     * 
     * @return true if the configuration was applied successfully, false otherwise
     */
    virtual bool LoadConfig(const json& config) = 0;
};
