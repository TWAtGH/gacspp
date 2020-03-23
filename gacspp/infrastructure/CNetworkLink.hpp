#pragma once

#include <vector>

#include "common/constants.h"

class CStorageElement;


class CNetworkLink
{
public:
    CNetworkLink(const SpaceType bandwidthBytesPerSecond, CStorageElement* const srcStorageElement, CStorageElement* const dstStorageElement);

    inline auto GetId() const -> IdType
    {return mId;}

    auto GetSrcStorageElement() const -> CStorageElement*;
    auto GetDstStorageElement() const -> CStorageElement*;

private:
    IdType mId;
    CStorageElement* mSrcStorageElement;
    CStorageElement* mDstStorageElement;

public:
    std::uint64_t mNumDoneTransfers = 0;
    std::uint64_t mNumFailedTransfers = 0;
    std::uint32_t mNumActiveTransfers = 0;

    std::uint32_t mMaxNumActiveTransfers = 0;

    SpaceType mUsedTraffic = 0;
    SpaceType mBandwidthBytesPerSecond;
};
