/**
 * @file   CConfigManager.hpp
 * @brief  Provides a helper class to ease config file loading
 *
 * @author Tobias Wegner
 * @date   March 2022
 */

#pragma once

#include <filesystem>

#include "third_party/nlohmann/json_fwd.hpp"

using nlohmann::json;

/**
* @brief Singleton class that provides helper functions to load profiles and configs
*
* The class knows two directories. One for configuration files one for profile directories.
* The paths are set at program startup. Furthermore, the class provides methods to load
* configs or profiles by name. Loading configurations using this class will automatically
* resolve nested json files.
*/
class CConfigManager
{
private:
    CConfigManager() = default;
    CConfigManager(const CConfigManager&) = delete;
    CConfigManager& operator=(const CConfigManager&) = delete;
    CConfigManager(const CConfigManager&&) = delete;
    CConfigManager& operator=(const CConfigManager&&) = delete;

public:
    /**
    * @brief path to the directory containing configuration files
    */
    std::filesystem::path mConfigDirPath;

    /**
    * @brief path to the directory containing profiles
    */
    std::filesystem::path mProfileDirPath;

    /**
    * @brief Singleton instance getter
    */
    static auto GetRef() -> CConfigManager&;


    /**
    * @brief Resolves a nested file path in the passed in json object
    * 
    * @param obj the json object to search in
    * 
    * @return the file name found in the given json object
    * 
    * Used mainly internally. If a json object within file A is configured to include data from file B,
    * then this function can be called with the object from file A and will return the file name of B.
    */
    auto GetFileNameFromObj(const json& obj) const -> std::filesystem::path;


    /**
    * @brief Tries loading a file and parsing its content into a json object
    *
    * @param outputJson OUT: contains the parsed json data if the method returns successfully
    * @param filePath the path of the file to load
    *
    * @return true if the file was opened successfully, false otherwise
    */
    bool TryLoadJson(json& outputJson, const std::filesystem::path& filePath) const;

    /**
    * @brief Tries loading a configuration file and parsing its content into a json object
    *
    * @param outputJson OUT: contains the parsed json data if the method returns successfully
    * @param fileName the name of the configuration file. This must be relative to mConfigDirPath
    *
    * @return true if the file was opened successfully, false otherwise
    */
    bool TryLoadCfg(json& outputJson, const std::filesystem::path& fileName) const;

    /**
    * @brief Tries loading a profile file and parsing its content into a json object
    *
    * @param outputJson OUT: contains the parsed json data if the method returns successfully
    * @param fileName the name of the profile file. This must be relative to mProfileDirPath
    *
    * @return true if the file was opened successfully, false otherwise
    */
    bool TryLoadProfileCfg(json& outputJson, const std::filesystem::path& fileName) const;
};
