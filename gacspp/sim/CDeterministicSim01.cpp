#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "CDeterministicSim01.hpp"

#include "infrastructure/CRucio.hpp"
#include "infrastructure/CStorageElement.hpp"

#include "CommonScheduleables.hpp"

#include "third_party/json.hpp"



bool CDeterministicSim01::SetupDefaults(const json& profileJson)
{
    if(!CDefaultBaseSim::SetupDefaults(profileJson))
        return false;

    CStorageElement* tapeStorageElement = nullptr;
    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
        for(const std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
            if(storageElement->GetName() == "BNL_DATATAPE")
                tapeStorageElement = storageElement.get();

    std::unordered_map<std::string, std::shared_ptr<SFile>> lfnToFile;
    std::uint64_t numReplicasCreated = 0;
    try
    {
        std::uint32_t fileTmplIdx = profileJson.at("fileDataFileFirstIdx").get<std::uint32_t>();
        const std::uint32_t width = profileJson.at("fileDataFilePathTmplWidth").get<std::uint32_t>();
        const std::string filePathTmpl = profileJson.at("fileDataFilePathTmpl").get<std::string>();
        const std::string filePathTmplFront = filePathTmpl.substr(0, filePathTmpl.find_first_of('$'));
        const std::string filePathTmplBack = filePathTmpl.substr(filePathTmpl.find_last_of('$')+1, std::string::npos);

        std::stringstream filePathBuilder;
        filePathBuilder << std::setfill('0') << filePathTmplFront << std::setw(width) << (fileTmplIdx++) << filePathTmplBack;

        std::ifstream fileDataFile(filePathBuilder.str());
        while(fileDataFile)
        {
            json fileDataJson;
            fileDataFile >> fileDataJson;

            for(const json& fileData : fileDataJson)
            {
                const std::string lfn = fileData.at("LFN").get<std::string>();
                const SpaceType filesize = fileData.at("FSIZE").get<SpaceType>();
                std::shared_ptr<SFile> newFile = mRucio->CreateFile(filesize, 0, SECONDS_PER_MONTH * 13);
                if(lfnToFile.insert({lfn, newFile}).second)
                {
                    if(!tapeStorageElement->CreateReplica(newFile, 0))
                        numReplicasCreated += 1;
                    else
                        std::cout<<"Failed to create replica of: "<<lfn<<std::endl;
                }
                else
                    std::cout<<"Double insertion attemp of: "<<lfn<<std::endl;
            }

            filePathBuilder.str("");
            filePathBuilder << filePathTmplFront << std::setw(width) << (fileTmplIdx++) << filePathTmplBack;
            fileDataFile.open(filePathBuilder.str());
        }
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to get fileDataFilePathTmpl: " << error.what() << std::endl;
    }

    std::cout<<"Num imported replicas: "<<numReplicasCreated<<std::endl;

    //create CDeterministicTransferGen and set first file

    //deletion?

    return true;
}
