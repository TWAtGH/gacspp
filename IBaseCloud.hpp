#pragma once

#include <string>
#include <memory>
#include <utility>
#include <vector>

#include "constants.h"

#include "IConfigConsumer.hpp"

class ISite;

class ICloudBill
{
public:
    virtual std::string ToString() const = 0;
};

class IBaseCloud : public IConfigConsumer
{
private:
	std::string mName;

public:
	std::vector<std::unique_ptr<ISite>> mRegions;

	IBaseCloud(std::string&& name);
    virtual ~IBaseCloud();

	virtual auto CreateRegion(std::string&& name,
                              std::string&& locationName,
                              const std::uint8_t multiLocationIdx,
                              double storagePriceCHF,
                              std::string&& skuId) -> ISite* = 0;

	virtual auto ProcessBilling(TickType now) -> std::unique_ptr<ICloudBill> = 0;
	virtual void SetupDefaultCloud() = 0;

	inline auto GetName() const -> const std::string&
	{return mName;}
};
