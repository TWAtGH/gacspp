#include <cassert>

#include "ISite.hpp"
#include "CNetworkLink.hpp"



CNetworkLink::CNetworkLink(const std::uint32_t bandwidth, ISite* srcSite, ISite* dstSite)
	: mId(GetNewId()),
      mSrcSite(srcSite),
      mDstSite(dstSite),
      mBandwidth(bandwidth)
{}

auto CNetworkLink::GetSrcSite() const -> ISite*
{return mSrcSite;}
auto CNetworkLink::GetDstSite() const -> ISite*
{return mDstSite;}
auto CNetworkLink::GetSrcSiteId() const -> IdType
{return mSrcSite->GetId();}
auto CNetworkLink::GetDstSiteId() const -> IdType
{return mDstSite->GetId();}
