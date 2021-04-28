#pragma once

#include <memory>
#include <string>
#include <vector>

#include "ISite.hpp"

#include "common/constants.h"
#include "common/IConfigConsumer.hpp"

class IRucioActionListener;
class CReaper;
struct SFile;
struct SReplica;


class CGridSite : public ISite
{
public:
    using ISite::ISite;

    virtual ~CGridSite();


    auto CreateStorageElement(std::string&& name, bool allowDuplicateReplicas = false, SpaceType limit = 0) -> CStorageElement*;
    auto GetStorageElements() const -> std::vector<CStorageElement*> override;

    std::vector<std::unique_ptr<CStorageElement>> mStorageElements;
};



class CRucio : public IConfigConsumer
{
private:
    std::unique_ptr<CReaper> mReaper;
    std::vector<std::unique_ptr<SFile>> mFiles;

public:
    std::vector<std::unique_ptr<CGridSite>> mGridSites;

    std::vector<IRucioActionListener*> mActionListener;

    CRucio();
    ~CRucio();

    void ReserveFileSpace(std::size_t amount);
    auto CreateFile(SpaceType size, TickType now, TickType lifetime) -> SFile*;
    
    void RemoveFile(SFile* file, TickType now);
    void RemoveAllFiles(TickType now);
    auto RemoveExpiredReplicasFromFile(SFile* file, TickType now) -> std::size_t;
    auto ExtractExpiredReplicasFromFile(SFile* file, TickType now) -> std::vector<SReplica*>;

    auto RunReaper(TickType now)->std::size_t;

    auto GetFiles() const -> const std::vector<std::unique_ptr<SFile>>&
    {return mFiles;}

    auto CreateGridSite(std::string&& name, std::string&& locationName, std::uint8_t multiLocationIdx) -> CGridSite*;

    auto GetStorageElementByName(const std::string& name) const -> CStorageElement*;

    bool LoadConfig(const json& config) final;
};
