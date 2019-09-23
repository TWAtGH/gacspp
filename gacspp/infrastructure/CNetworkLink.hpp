#pragma once

#include <vector>

#include "common/constants.h"

class ISite;

class CNetworkLink
{
private:
    //monitoring
    IdType mId;
    ISite* mSrcSite;
    ISite* mDstSite;

public:
    std::uint32_t mDoneTransfers = 0;
    std::uint32_t mFailedTransfers = 0;

public:
    CNetworkLink(const std::uint32_t bandwidth, ISite* srcSite, ISite* dstSite);

    inline auto GetId() const -> IdType
    {return mId;}
    auto GetSrcSite() const -> ISite*;
    auto GetDstSite() const -> ISite*;
    auto GetSrcSiteId() const -> IdType;
    auto GetDstSiteId() const -> IdType;

    inline auto GetWeight() const -> double
    {
        //todo, since traffic cost stored in regions now
        return 0;
    }

    std::uint64_t mUsedTraffic = 0;
    std::uint32_t mNumActiveTransfers = 0;
    std::uint32_t mBandwidth;
};
