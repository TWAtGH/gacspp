#pragma once

#include <string>
#include <memory>
#include <utility>
#include <vector>

#include "constants.h"

#include "IConfigConsumer.hpp"

class ISite;



class IBaseCloud : public IConfigConsumer
{
private:
	std::string mName;

public:
	std::vector<std::unique_ptr<ISite>> mRegions;

	IBaseCloud(std::string&& name);
    virtual ~IBaseCloud();

	virtual auto CreateRegion(const std::uint32_t multiLocationIdx,
                              std::string&& name,
                              const std::string& locationName,
                              double storagePriceCHF,
                              std::string&& skuId) -> ISite* = 0;
                              
	virtual auto ProcessBilling(TickType now) -> std::pair<double, std::pair<double, double>> = 0;
	virtual void SetupDefaultCloud() = 0;

	inline auto GetName() const -> const std::string&
	{return mName;}
};
