#include <cassert>

#include "ISite.hpp"

#include "CLinkSelector.hpp"
#include "CStorageElement.hpp"



ISite::ISite(std::string&& name, std::string&& locationName)
	: mId(GetNewId()),
      mName(std::move(name)),
      mLocationName(std::move(locationName))
{}

ISite::~ISite() = default;

auto ISite::CreateLinkSelector(ISite* const dstSite, const std::uint32_t bandwidth) -> CLinkSelector*
{
    auto result = mDstSiteIdToLinkSelectorIdx.insert({dstSite->mId, mLinkSelectors.size()});
    assert(result.second);
    CLinkSelector* newLinkSelector = new CLinkSelector(bandwidth, this, dstSite);
    mLinkSelectors.emplace_back(newLinkSelector);
    return newLinkSelector;
}

auto ISite::GetLinkSelector(const ISite* const dstSite) -> CLinkSelector*
{
    auto result = mDstSiteIdToLinkSelectorIdx.find(dstSite->mId);
    if(result == mDstSiteIdToLinkSelectorIdx.end())
        return nullptr;
    return mLinkSelectors[result->second].get();
}
