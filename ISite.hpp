#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "constants.h"

class CLinkSelector;
class CStorageElement;



// 0 = asia
// 1 = australia-southeast1
// 2 = europe
// 3 = southamerica-east1
// 4 = us

class ISite
{
public:
	ISite(std::string&& name, std::string&& locationName, const std::uint8_t multiLocationIdx);
	virtual ~ISite();

	ISite(ISite&&) = default;
	ISite& operator=(ISite&&) = default;

	ISite(ISite const&) = delete;
	ISite& operator=(ISite const&) = delete;

	inline bool operator==(const ISite& b) const
	{return mId == b.mId;}
	inline bool operator!=(const ISite& b) const
	{return mId != b.mId;}

	virtual auto CreateLinkSelector(ISite* const dstSite, const std::uint32_t bandwidth) -> CLinkSelector*;
    virtual auto CreateStorageElement(std::string&& name) -> CStorageElement* = 0;

	auto GetLinkSelector(const ISite* const dstSite) -> CLinkSelector*;

	inline auto GetId() const -> IdType
	{return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetLocationName() const -> const std::string&
    {return mLocationName;}
    inline auto GetMultiLocationIdx() const -> std::uint8_t
    {return mMultiLocationIdx;}

    std::vector<std::unique_ptr<CLinkSelector>> mLinkSelectors;

private:
	IdType mId;
    std::string mName;
    std::string mLocationName;
    std::uint8_t mMultiLocationIdx;

protected:
	std::unordered_map<IdType, std::size_t> mDstSiteIdToLinkSelectorIdx;

public:
    std::unordered_map<std::string, std::string> mCustomConfig;
};
