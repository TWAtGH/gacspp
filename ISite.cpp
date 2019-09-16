#include <cassert>

#include "ISite.hpp"

#include "CNetworkLink.hpp"
#include "CStorageElement.hpp"



ISite::ISite(std::string&& name, std::string&& locationName, const std::uint8_t multiLocationIdx)
	: mId(GetNewId()),
      mName(std::move(name)),
      mLocationName(std::move(locationName)),
      mMultiLocationIdx(multiLocationIdx)
{}

ISite::~ISite() = default;

auto ISite::CreateNetworkLink(ISite* const dstSite, const std::uint32_t bandwidth) -> CNetworkLink*
{
    auto result = mDstSiteIdToNetworkLinkIdx.insert({dstSite->mId, mNetworkLinks.size()});
    assert(result.second);
    CNetworkLink* newNetworkLink = new CNetworkLink(bandwidth, this, dstSite);
    mNetworkLinks.emplace_back(newNetworkLink);
    return newNetworkLink;
}

auto ISite::GetNetworkLink(const ISite* const dstSite) -> CNetworkLink*
{
    auto result = mDstSiteIdToNetworkLinkIdx.find(dstSite->mId);
    if(result == mDstSiteIdToNetworkLinkIdx.end())
        return nullptr;
    return mNetworkLinks[result->second].get();
}

auto ISite::GetNetworkLink(const ISite* const dstSite) const -> const CNetworkLink*
{
	auto result = mDstSiteIdToNetworkLinkIdx.find(dstSite->mId);
	if (result == mDstSiteIdToNetworkLinkIdx.end())
		return nullptr;
	return mNetworkLinks[result->second].get();
}
