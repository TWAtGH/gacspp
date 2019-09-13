#include <fstream>
#include <iostream>

#include "constants.h"
#include "CConfigLoader.hpp"
#include "json.hpp"


auto CConfigLoader::GetRef() -> CConfigLoader&
{
    static CConfigLoader mInstance;
    return mInstance;
}

bool CConfigLoader::TryLoadConfig(const json& jsonRoot)
{
    if(mConfigConsumer.empty())
        std::cout << "No config consumers registered" << std::endl;

    for(json::const_iterator jsonRootIt=jsonRoot.cbegin(); jsonRootIt!=jsonRoot.cend(); ++jsonRootIt)
    {
        bool wasConsumed = false;
        auto resultIt = jsonRootIt.value().find(JSON_FILE_IMPORT_KEY);
        if(jsonRootIt.value().is_object() && (resultIt != jsonRootIt.value().end()))
        {
            if(!resultIt.value().is_string())
            {
                std::cout << "Expected value of _file_ field to be of type string. Ignoring entry:" << std::endl;
                std::cout << resultIt->dump(JSON_DUMP_SPACES) << std::endl;
                continue;
            }
            std::filesystem::path curPathBefore = mCurrentDirectory;
            wasConsumed = TryLoadConfig(mCurrentDirectory / resultIt.value().get<std::string>());
            mCurrentDirectory = curPathBefore;
        }
        else
        {
            const json consumableJson = { {jsonRootIt.key(), jsonRootIt.value()} };
            for(IConfigConsumer* consumer : mConfigConsumer)
                wasConsumed = wasConsumed || consumer->TryConsumeConfig(consumableJson);
        }

        if(!wasConsumed)
        {
            std::cout << "Didnt consume json entry:" << std::endl;
            std::cout << jsonRootIt->dump(JSON_DUMP_SPACES) << std::endl;
        }
    }

    return true;
}

bool CConfigLoader::TryLoadConfig(const std::filesystem::path& path)
{
    auto insertResult = mLoadedFiles.insert(path.string());
    if(!insertResult.second)
    {
        std::cout << "Preventing file from being loaded twice: " << path << std::endl;
        return false;
    }

    json jsonRoot;

    {
        std::ifstream configFile(path);
        if(!configFile)
        {
            std::cout << "Unable to load config: " << path << std::endl;
            return false;
        }
        configFile >> jsonRoot;
    }

    mCurrentDirectory = path;
    mCurrentDirectory.remove_filename();

    return TryLoadConfig(jsonRoot);
}
