#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/constants.h"

class CNetworkLink;
class CStorageElement;
class IStorageElementDelegate;



class ISite
{
public:
    ISite(std::string&& name, std::string&& locationName, const std::uint8_t multiLocationIdx);

    ISite(ISite&&) = delete;
    ISite& operator=(ISite&&) = delete;
    ISite(const ISite&) = delete;
    ISite& operator=(const ISite&) = delete;

    virtual ~ISite();


    inline bool operator==(const ISite& b) const
    {return mId == b.mId;}
    inline bool operator!=(const ISite& b) const
    {return mId != b.mId;}

    virtual auto CreateNetworkLink(ISite* const dstSite, const std::uint32_t bandwidth) -> CNetworkLink*;
    virtual auto CreateStorageElement(std::string&& name, bool forbidDuplicatedReplicas=true) -> CStorageElement* = 0;

    auto GetNetworkLink(const ISite* const dstSite) -> CNetworkLink*;
    auto GetNetworkLink(const ISite* const dstSite) const -> const CNetworkLink*;

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetLocationName() const -> const std::string&
    {return mLocationName;}
    inline auto GetMultiLocationIdx() const -> std::uint8_t
    {return mMultiLocationIdx;}

    std::vector<std::unique_ptr<CNetworkLink>> mNetworkLinks;

private:
    IdType mId;
    std::string mName;
    std::string mLocationName;
    std::uint8_t mMultiLocationIdx;

protected:
    std::unordered_map<IdType, std::size_t> mDstSiteIdToNetworkLinkIdx;

public:
    std::unordered_map<std::string, std::string> mCustomConfig;
};
