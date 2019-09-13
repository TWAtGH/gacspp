#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "json.hpp"
#include "CAdvancedSim.hpp"
#include "COutput.hpp"
#include "CSimpleSim.hpp"

using nlohmann::json;


int main()
{
    const auto startTime = std::chrono::high_resolution_clock::now();
    COutput& output = COutput::GetRef();

    const std::filesystem::path configDirPath = std::filesystem::current_path() / "config";
    json configJson;
    {
        const std::filesystem::path baseConfigFilePath = configDirPath / "simconfig.json";
        std::ifstream configFileStream(baseConfigFilePath.string());
        if(!configFileStream)
            std::cout << "Unable to locate config file: " << baseConfigFilePath << std::endl;
        else
            configFileStream >> configJson;
    }

    {
        std::size_t insertQueryBufferLen = 250000;
        std::string dbConnectionString;
        std::filesystem::path dbInitFilePath;
        auto outputConfig = configJson.find("output");
        if(outputConfig != configJson.end())
        {
            auto prop = outputConfig->find("dbConnectionFile");
            if(prop != outputConfig->end())
			{
				const std::filesystem::path dbConnectionFilePath = (configDirPath / prop->get<std::string>());
				std::ifstream dbConnectionFileStream(dbConnectionFilePath.string());
				json dbConnectionFileJson;
				if (!dbConnectionFileStream)
					std::cout << "Unable to locate db connection file: " << dbConnectionFilePath.string() << std::endl;
				else
					dbConnectionFileStream >> dbConnectionFileJson;
				dbConnectionString = dbConnectionFileJson["connectionStr"].get<std::string>();
			}

            prop = outputConfig->find("dbInitFileName");
            if(prop != outputConfig->end())
                dbInitFilePath = (configDirPath / prop->get<std::string>());

            prop = outputConfig->find("insertQueryBufferLen");
            if(prop != outputConfig->end())
                insertQueryBufferLen = prop->get<std::size_t>();
        }

        if(!dbInitFilePath.empty())
        {
            json dbInitJson;
            std::ifstream dbInitFileStream(dbInitFilePath.string());
            if(!dbInitFileStream)
                std::cout << "Unable to open db init file: " << dbInitFilePath << std::endl;
            else
                dbInitFileStream >> dbInitJson;

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
        auto prop = configJson.find("maxTick");
        if(prop != configJson.end())
            maxTick = prop->get<TickType>();
    }
    std::cout<<"MaxTick="<<maxTick<<std::endl;

    std::cout<<"Setting up sim..."<<std::endl;
    //auto sim = std::make_unique<CSimpleSim>();
    auto sim = std::make_unique<CAdvancedSim>();
    sim->SetupDefaults();

    std::cout<<"Running sim..."<<std::endl;
    output.StartConsumer();
    sim->Run(maxTick);

    std::cout<<"Finalising database..."<<std::endl;
    output.Shutdown();

    const auto runTime = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::high_resolution_clock::now() - startTime);
    std::cout<<"Simulation took "<<runTime.count()<<"s"<<std::endl;

	int a;
	std::cin >> a;
}
