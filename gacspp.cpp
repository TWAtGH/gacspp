#include <iomanip>
#include <iostream>

#include "json.hpp"
#include "CConfigManager.hpp"
#include "CDefaultSim.hpp"
#include "COutput.hpp"



int main(int argc, char** argv)
{
    const auto startTime = std::chrono::high_resolution_clock::now();

	//base paths
	CConfigManager& configManager = CConfigManager::GetRef();
	configManager.mConfigDirPath = std::filesystem::current_path() / "config";
	configManager.mProfileDirPath = configManager.mConfigDirPath / "profiles";


	//try to load main config file
    json configJson;
	configManager.TryLoadCfg(configJson, "simconfig.json");


	//try to load sim profile
	json profileJson;
	if (argc > 1)
		configManager.TryLoadProfileCfg(profileJson, argv[1]);

	if (profileJson.empty() && configJson.contains("profile"))
		configManager.TryLoadProfileCfg(profileJson, configJson["profile"].get<std::string>());

	if (profileJson.empty())
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

    std::cout<<"Setting up sim..."<<std::endl;
    std::unique_ptr<IBaseSim> sim = std::make_unique<CDefaultSim>();
    sim->SetupDefaults(profileJson);

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
