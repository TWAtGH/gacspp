#include <cassert>

#include "ISite.hpp"
#include "CNetworkLink.hpp"

#include "common/utils.hpp"


CNetworkLink::CNetworkLink(SpaceType bandwidthBytesPerSecond, CStorageElement* srcStorageElement, CStorageElement* dstStorageElement)
    : mId(GetNewId()),
      mSrcStorageElement(srcStorageElement),
      mDstStorageElement(dstStorageElement),
      mBandwidthBytesPerSecond(bandwidthBytesPerSecond)
{
}

auto CNetworkLink::GetSrcStorageElement() const -> CStorageElement*
{return mSrcStorageElement;}
auto CNetworkLink::GetDstStorageElement() const -> CStorageElement*
{return mDstStorageElement;}