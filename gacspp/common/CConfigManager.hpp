#pragma once

#include <filesystem>

#include "third_party/nlohmann/json_fwd.hpp"

using nlohmann::json;


class CConfigManager
{
private:
    CConfigManager() = default;
    CConfigManager(const CConfigManager&) = delete;
    CConfigManager& operator=(const CConfigManager&) = delete;
    CConfigManager(const CConfigManager&&) = delete;
    CConfigManager& operator=(const CConfigManager&&) = delete;

public:
    std::filesystem::path mConfigDirPath;
    std::filesystem::path mProfileDirPath;

    static auto GetRef() -> CConfigManager&;

    auto GetFileNameFromObj(const json& obj) const->std::filesystem::path;

    bool TryLoadJson(json& outputJson, const std::filesystem::path& filePath) const;
    bool TryLoadCfg(json& outputJson, const std::filesystem::path& fileName) const;
    bool TryLoadProfileCfg(json& outputJson, const std::filesystem::path& fileName) const;
};
