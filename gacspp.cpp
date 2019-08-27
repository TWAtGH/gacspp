#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "json.hpp"
#include "CAdvancedSim.hpp"
#include "COutput.hpp"
#include "CSimpleSim.hpp"


int main()
{
    COutput& output = COutput::GetRef();

    nlohmann::json configJson;
    {
        const std::string configFilePath(std::filesystem::current_path() / "config" / "simconfig.json");
        std::ifstream configFileStream(configFilePath);
        if(!configFileStream)
        {
            std::cout << "Unable to locate config file: " << configFilePath << std::endl;
        }
        else
            configFileStream >> configJson;
    }

    {

        /*
        bool keepInMemory = false;
        std::filesystem::path outputBaseDirPath = std::filesystem::current_path() / "output" / "";
        std::filesystem::create_directories(outputBaseDirPath);

        std::stringstream filenameTimePrefix;
        auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        filenameTimePrefix << std::put_time(std::localtime(&now), "%y%j_%H%M%S");

        std::string outputFilename;

        auto outputConfig = configJson.find("output");
        if(outputConfig != configJson.end())
        {
            auto prop = outputConfig->find("keepInMemory");
            if(prop != outputConfig->end())
                if(prop->is_boolean())
                    keepInMemory = prop->get<bool>();

            prop = outputConfig->find("baseDirPath");
            if(prop != outputConfig->end())
            {
                outputBaseDirPath = prop->get<std::string>();
                outputBaseDirPath /= "";
            }

            prop = outputConfig->find("filenamePrefix");
            if(prop != outputConfig->end())
                filenameTimePrefix.str(prop->get<std::string>());

            prop = outputConfig->find("filename");
            if(prop != outputConfig->end())
                outputFilename = prop->get<std::string>();
        }

        std::filesystem::path outputFilePath;
        if(!outputFilename.empty())
        {
            keepInMemory = true;
            outputFilePath = outputBaseDirPath / (filenameTimePrefix.str() + outputFilename);
        }

        if (keepInMemory)
            std::cout<<"DB in memory"<<std::endl;
        if(!outputFilePath.empty())
            std::cout<<"Output file: "<<outputFilePath<<std::endl;
*/
        if(!output.Initialise(""))
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

    //auto sim = std::make_unique<CSimpleSim>();
    auto sim = std::make_unique<CAdvancedSim>();
    sim->SetupDefaults();

    output.StartConsumer();
    sim->Run(maxTick);
    output.Shutdown();

	int a;
	std::cin >> a;
}
