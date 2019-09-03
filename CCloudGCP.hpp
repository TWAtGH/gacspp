#pragma once

#include "IBaseCloud.hpp"
#include "ISite.hpp"

#include "CStorageElement.hpp"

namespace gcp
{
	class CRegion;
	class CBucket : public CStorageElement
	{
	private:
        CRegion* mRegion;
        TickType mTimeLastCostUpdate = 0;
        double mStorageCosts = 0;
        double mOperationCosts = 0;

	public:

		CBucket(std::string&& name, CRegion* region);
		CBucket(CBucket&&) = default;

        virtual void OnOperation(const CStorageElement::OPERATION op) final;

		virtual void OnIncreaseReplica(std::uint64_t amount, TickType now) final;
		virtual void OnRemoveReplica(const SReplica* replica, TickType now) final;

		double CalculateStorageCosts(TickType now);
	};

	class CRegion : public ISite
	{
	public:

		CRegion(std::string&& name, std::string&& locationName, const std::uint8_t multiLocationIdx, const double storagePrice, std::string&& skuId);

		auto CreateStorageElement(std::string&& name) -> CBucket* final;
		double CalculateStorageCosts(TickType now);
		double CalculateNetworkCosts(double& sumUsedTraffic, std::uint64_t& sumDoneTransfers);

		inline auto GetStoragePrice() const -> double
		{return mStoragePrice;}

        std::vector<std::unique_ptr<CBucket>> mStorageElements;

    private:
		std::string mSKUId;
		double mStoragePrice = 0;
	};

	class CCloud final : public IBaseCloud
	{
	public:
		using IBaseCloud::IBaseCloud;

		auto CreateRegion(std::string&& name,
                          std::string&& locationName,
                          const std::uint8_t multiLocationIdx,
                          const double storagePrice,
                          std::string&& skuId) -> CRegion* final;

		auto ProcessBilling(TickType now) -> std::pair<double, std::pair<double, double>> final;
		void SetupDefaultCloud() final;

        bool TryConsumeConfig(const nlohmann::json& json) final;
	};
}
