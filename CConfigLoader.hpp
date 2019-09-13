#pragma once

#include <filesystem>
#include <string>
#include <unordered_set>
#include <vector>

#include "IConfigConsumer.hpp"



class CConfigLoader
{
private:
    CConfigLoader() = default;

    std::filesystem::path mCurrentDirectory;

public:
    std::unordered_set<std::string> mLoadedFiles;
    std::vector<IConfigConsumer*> mConfigConsumer;

public:
    CConfigLoader(const CConfigLoader&) = delete;
    CConfigLoader& operator=(const CConfigLoader&) = delete;
    CConfigLoader(const CConfigLoader&&) = delete;
    CConfigLoader& operator=(const CConfigLoader&&) = delete;

    static auto GetRef() -> CConfigLoader&;

    bool TryLoadConfig(const std::filesystem::path& path);
    bool TryLoadConfig(const json& jsonRoot);
};
