#pragma once

#include "IBaseCloud.hpp"
#include "ISite.hpp"

#include "CStorageElement.hpp"

namespace gcp
{
    class CCloudBill : public ICloudBill
    {
    private:
        double mStorageCost;
        double mNetworkCost;
        double mTraffic;
        double mOperationCost;

    public:
        CCloudBill(double storageCost, double networkCost, double traffic, double operationCost);
        virtual std::string ToString() const final;
    };

	class CRegion;
	class CBucket : public CStorageElement
	{
	private:
        TickType mTimeLastCostUpdate = 0;
        double mStorageCosts = 0;
        double mOperationCosts = 0;

	public:
        std::vector<std::pair<std::uint32_t, std::int32_t>> mStorageRates;

        using CStorageElement::CStorageElement;

		CBucket(CBucket&&) = default;

        virtual void OnOperation(const CStorageElement::OPERATION op) final;

		virtual void OnIncreaseReplica(std::uint64_t amount, TickType now) final;
		virtual void OnRemoveReplica(const SReplica* replica, TickType now) final;

		auto CalculateStorageCosts(TickType now) -> double;
        auto CalculateOperationCosts(TickType now) -> double;

        auto GetCurStoragePrice() const -> double;
	};

	class CRegion : public ISite
	{
	public:
        using ISite::ISite;

		auto CreateStorageElement(std::string&& name) -> CBucket* final;
		double CalculateStorageCosts(TickType now);
		double CalculateOperationCosts(TickType now);
		double CalculateNetworkCosts(double& sumUsedTraffic, std::uint64_t& sumDoneTransfers);

        std::vector<std::unique_ptr<CBucket>> mStorageElements;
	};

	class CCloud final : public IBaseCloud
	{
	public:
		using IBaseCloud::IBaseCloud;

        virtual ~CCloud();

		auto CreateRegion(std::string&& name,
                          std::string&& locationName,
                          const std::uint8_t multiLocationIdx) -> CRegion* final;

		auto ProcessBilling(TickType now) -> std::unique_ptr<ICloudBill> final;
		void SetupDefaultCloud() final;

        bool TryConsumeConfig(const json& config) final;

    private:
        json* mSKUSettings = nullptr;

        void LoadBaseSettingsJson(const json& config);
        void InitBucketBySKUId(CBucket* bucket, const std::string& skuId);
	};
}
