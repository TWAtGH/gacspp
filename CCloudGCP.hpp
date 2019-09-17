#pragma once

#include "IBaseCloud.hpp"
#include "ISite.hpp"

#include "CStorageElement.hpp"

namespace gcp
{
	typedef std::vector<std::pair<std::uint64_t, std::int32_t>> TieredPriceType;

    class CCloudBill : public ICloudBill
    {
    private:
        double mStorageCost;
        double mNetworkCost;
        double mTraffic;
        double mOperationCost;
		std::size_t mNumClassA;
		std::size_t mNumClassB;

    public:
        CCloudBill(double storageCost, double networkCost, double traffic, double operationCost, std::size_t numClassA, std::size_t numClassB);
        virtual std::string ToString() const final;
    };

	class CRegion;
	class CBucket : public CStorageElement
	{
	private:
        TickType mTimeLastCostUpdate = 0;
        double mStorageCosts = 0;
		std::size_t mNumClassA;
		std::size_t mNumClassB;

	public:
		using CStorageElement::CStorageElement;

		CBucket(CBucket&&) = default;

        virtual void OnOperation(const CStorageElement::OPERATION op) final;

		virtual void OnIncreaseReplica(std::uint64_t amount, TickType now) final;
		virtual void OnRemoveReplica(const SReplica* replica, TickType now) final;

		auto CalculateStorageCosts(TickType now) -> double;
        auto CalculateOperationCosts() -> double;

        auto GetCurStoragePrice() const -> double;

		inline auto GetNumClassA() const -> std::size_t
		{return mNumClassA;}
		inline auto GetNumClassB() const -> std::size_t
		{return mNumClassB;}
	};

	class CRegion : public ISite
	{
	public:
		using ISite::ISite;

		auto CreateStorageElement(std::string&& name, const TickType accessLatency) -> CBucket* final;
		auto CalculateStorageCosts(TickType now) -> double;
		auto CalculateOperationCosts(std::size_t& numClassA, std::size_t& numClassB) -> double;
		auto CalculateNetworkCosts(double& sumUsedTraffic, std::uint64_t& sumDoneTransfers) -> double;

        std::vector<std::unique_ptr<CBucket>> mStorageElements;
		std::unordered_map<IdType, TieredPriceType> mNetworkLinkIdToPrice;
		TieredPriceType mStoragePrice;
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
		void InitialiseNetworkLinks() final;

        bool LoadConfig(const json& config) final;

    private:
		//todo: use smart pointers (destructor invisible?)
		json* mSKUSettings = nullptr;
		json* mNetworkPrices = nullptr;

		auto GetTieredRateFromSKUId(const std::string& skuId) const -> TieredPriceType;
	};

	class CCloudFactory : public ICloudFactory
	{
	private:
		static ICloudFactory* mInstance;

	public:
		CCloudFactory();
		~CCloudFactory();

		auto CreateCloud(std::string&& cloudName) const->std::unique_ptr<IBaseCloud> final;
	};
}
