/**
 * @file   CNetworkLink.hpp
 * @brief  Contains the class that represents a network link
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * The CNetworkLink class contains several data members to describe a network link and
 * keep track of its runtime data.
 */
#pragma once

#include <vector>

#include "common/constants.h"

class CStorageElement;

/**
* @brief Class that represents a point to point network connection between two storage elements
*
* The main member variables describing a network link are an unique id, the bandwidth, the source,
* and the destination storage element. In addition, this class provides several more variables to
* track statistics of done/failed transfers, transferred volume, etc.
*/
class CNetworkLink
{
public:
    /**
    * @brief Initialises the object with the passed in values
    *
    * @param bandwidthBytesPerSecond the bandwidth of this network link in bytes per second
    * @param srcStorageElement a valid pointer to the source storage element of this network link
    * @param dstStorageElement a valid pointer to the destination storage element of this network link
    */
    CNetworkLink(SpaceType bandwidthBytesPerSecond, CStorageElement* srcStorageElement, CStorageElement* dstStorageElement);

    inline auto GetId() const -> IdType
    {return mId;}


    /**
    * @brief Getter for the pointer to the source storage element
    * 
    * @return the pointer to the source storage element
    */
    auto GetSrcStorageElement() const -> CStorageElement*;

    /**
    * @brief Getter for the pointer to the destination storage element
    *
    * @return the pointer to the destination storage element
    */
    auto GetDstStorageElement() const -> CStorageElement*;

private:
    /**
    * @brief The unique id of this network link. The id is unique across all object types.
    *
    * The id is automatically generated in the constructor.
    */
    IdType mId;

    /**
    * @brief Pointer to the source storage element
    */
    CStorageElement* mSrcStorageElement;

    /**
    * @brief Pointer to the destination storage element
    */
    CStorageElement* mDstStorageElement;

public:

    /**
    * @brief Counter of finished transfers
    */
    std::uint64_t mNumDoneTransfers = 0;

    /**
    * @brief Counter of failed transfers
    */
    std::uint64_t mNumFailedTransfers = 0;

    /**
    * @brief Counter of currently active transfers
    */
    std::uint32_t mNumActiveTransfers = 0;

    /**
    * @brief Maximum number of parallel active transfers
    */
    std::uint32_t mMaxNumActiveTransfers = 0;

    /**
    * @brief Amount of already transferred traffic
    */
    SpaceType mUsedTraffic = 0;

    /**
    * @brief Bandwidth of this network link
    */
    SpaceType mBandwidthBytesPerSecond;

    /**
    * @brief If true the bandwidth is interpreted as throughput and will not be divided by the number of active transfers
    */
    bool mIsThroughput = false;
};
