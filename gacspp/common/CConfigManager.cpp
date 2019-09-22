#include <fstream>
#include <iostream>

#include "CConfigManager.hpp"
#include "common/constants.h"
#include "third_party/json.hpp"


auto CConfigManager::GetRef() -> CConfigManager &
{
    static CConfigManager instance;
    return instance;
}

auto CConfigManager::GetFileNameFromObj(const json& obj) const -> std::filesystem::path
{
    std::filesystem::path fileName;

    // if there is no "config" obj search directly in the obj for JSON_FILE_IMPORT_KEY
    json::const_iterator cfgPropIt = obj.find("config");
    if (cfgPropIt == obj.cend())
        cfgPropIt = obj.cbegin();

    if (cfgPropIt->contains(JSON_FILE_IMPORT_KEY))
        fileName = cfgPropIt->find(JSON_FILE_IMPORT_KEY)->get<std::string>();

    return fileName;
}

bool CConfigManager::TryLoadJson(json& outputJson, const std::filesystem::path& filePath) const
{
    if (filePath.empty())
        return false;

    std::ifstream fileStream(filePath.string());
    if (!fileStream)
    {
        std::cout << "Unable to open json file: " << filePath << std::endl;
        return false;
    }

    fileStream >> outputJson;
    return true;
}

bool CConfigManager::TryLoadCfg(json& outputJson, const std::filesystem::path& fileName) const
{
    if (fileName.empty())
        return false;
    return TryLoadJson(outputJson, mConfigDirPath / fileName);
}

bool CConfigManager::TryLoadProfileCfg(json& outputJson, const std::filesystem::path& fileName) const
{
    if (fileName.empty())
        return false;
    return TryLoadJson(outputJson, mProfileDirPath / fileName);
}
