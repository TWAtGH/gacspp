#include <iomanip>
#include <iostream>

#include "CDefaultBaseSim.hpp"

#include "common/CConfigManager.hpp"

#include "output/COutput.hpp"

#include "third_party/nlohmann/json.hpp"



int main(int argc, char** argv)
{
    const auto startTime = std::chrono::high_resolution_clock::now();

    //base paths
    CConfigManager& configManager = CConfigManager::GetRef();
    configManager.mConfigDirPath = std::filesystem::current_path() / "config";


    //try to load main config file
    json configJson;
    configManager.TryLoadCfg(configJson, "simconfig.json");

    //try to load sim profile
    std::string profileDir;
    if (argc < 2)
    {
        try
        {
            profileDir = configJson.at("profile").get<std::string>();
        }
        catch (const json::out_of_range& error)
        {
            std::cout << "Failed to determine profile directory..." << std::endl;
            return 1;
        }
    }
    else
        profileDir = argv[1];

    configManager.mProfileDirPath = configManager.mConfigDirPath / "profiles" / profileDir;
    std::cout << "Using profile directory: " << configManager.mProfileDirPath << std::endl;
    system(("title " + profileDir).c_str());

    json profileJson;
    if (!configManager.TryLoadProfileCfg(profileJson, "profile.json"))
    {
        std::cout << "Failed to load a profile file..." << std::endl;
        return 1;
    }


    //init output
    COutput& output = COutput::GetRef();
    {
        std::size_t insertQueryBufferLen = 250000;
        std::string dbConnectionString;
        std::filesystem::path dbInitFileName;
        json::const_iterator outputPropIt = configJson.find("output");
        if(outputPropIt != configJson.cend())
        {
            if(outputPropIt->contains("dbConnectionFile"))
            {
                const std::filesystem::path connectionFileName = outputPropIt->find("dbConnectionFile")->get<std::string>();
                json dbConnectionFileJson;
                if(configManager.TryLoadCfg(dbConnectionFileJson, connectionFileName))
                {
                    if(dbConnectionFileJson.contains("connectionStr"))
                        dbConnectionString = dbConnectionFileJson.find("connectionStr")->get<std::string>();
                    else
                        std::cout << "Failed to locate connectionStr property in connection file: " << connectionFileName << std::endl;
                }
            }

            if(outputPropIt->contains("dbInitFileName"))
                dbInitFileName = outputPropIt->find("dbInitFileName")->get<std::string>();

            if(outputPropIt->contains("insertQueryBufferLen"))
                insertQueryBufferLen = outputPropIt->find("insertQueryBufferLen")->get<std::size_t>();
        }

        if(!dbInitFileName.empty())
        {
            json dbInitJson;
            configManager.TryLoadCfg(dbInitJson, dbInitFileName);

            for(auto& [key, value] : dbInitJson.items())
            {
                std::vector<std::string>* container = nullptr;
                if(key == "init")
                    container = &(output.mInitQueries);
                else if(key == "shutdown")
                    container = &(output.mShutdownQueries);
                else
                    continue;

                for(auto& query : value)
                    container->emplace_back(query.get<std::string>());
            }
        }

        if(!output.Initialise(dbConnectionString, insertQueryBufferLen))
        {
            std::cout << "Failed initialising output component" << std::endl;
            return 1;
        }
    }


    TickType maxTick = 3600 * 24 * 30;
    {
        json::const_iterator prop = profileJson.find("maxTick");
        if(prop != profileJson.cend())
            maxTick = prop->get<TickType>();
    }
    std::cout<<"MaxTick="<<maxTick<<std::endl;

    {
        std::cout<<"Setting up sim..."<<std::endl;
        std::unique_ptr<CDefaultBaseSim> sim = std::make_unique<CDefaultBaseSim>();
        if(!sim->SetupDefaults(profileJson))
        {
            std::cout<<"Setting up sim failed"<<std::endl;
            return 1;
        }

        std::cout<<"Running sim..."<<std::endl;
        output.StartConsumer();
        sim->Run(maxTick);
    }

    std::cout<<"Finalising database..."<<std::endl;
    output.Shutdown();

    const auto runTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::high_resolution_clock::now() - startTime);
    std::cout<<"Simulation took "<<runTime.count()<<"s"<<std::endl;

    return 0;
}
