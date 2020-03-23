#pragma once

#include <memory>
#include <string>
#include <vector>

#include "ISite.hpp"

#include "common/constants.h"
#include "common/IConfigConsumer.hpp"

struct SFile;
struct SReplica;
class CReaper;
class IFileActionListener;


class CGridSite : public ISite
{
public:
    using ISite::ISite;

    virtual ~CGridSite();


    auto CreateStorageElement(std::string&& name, bool allowDuplicateReplicas = false, SpaceType quota = 0) -> CStorageElement*;
    void GetStorageElements(std::vector<CStorageElement*>& storageElements);

    std::vector<std::unique_ptr<CStorageElement>> mStorageElements;
};



class CRucio : public IConfigConsumer
{
private:
    std::unique_ptr<CReaper> mReaper;

public:
    std::vector<std::shared_ptr<SFile>> mFiles;
    std::vector<std::unique_ptr<CGridSite>> mGridSites;

    std::vector<std::weak_ptr<IFileActionListener>> mFileActionListeners;

    CRucio();
    ~CRucio();

    auto CreateFile(const SpaceType size, const TickType now, const TickType lifetime) -> std::shared_ptr<SFile>;
    auto CreateGridSite(std::string&& name, std::string&& locationName, const std::uint8_t multiLocationIdx) -> CGridSite*;
    auto RunReaper(const TickType now) -> std::size_t;

    auto GetStorageElementByName(const std::string& name) -> CStorageElement*;

    bool LoadConfig(const json& config) final;
};
