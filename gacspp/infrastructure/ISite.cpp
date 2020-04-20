#include <cassert>

#include "ISite.hpp"
#include "CNetworkLink.hpp"
#include "CStorageElement.hpp"

#include "common/utils.hpp"


ISite::ISite(std::string&& name, std::string&& locationName, std::uint8_t multiLocationIdx)
    : mId(GetNewId()),
      mName(std::move(name)),
      mLocationName(std::move(locationName)),
      mMultiLocationIdx(multiLocationIdx)
{}

ISite::~ISite() = default;