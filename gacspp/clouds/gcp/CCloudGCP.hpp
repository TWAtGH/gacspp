#pragma once

#include "clouds/IBaseCloud.hpp"

#include "infrastructure/ISite.hpp"
#include "infrastructure/CStorageElement.hpp"


namespace gcp
{
    typedef std::vector<std::pair<std::uint32_t, long double>> TieredPriceType;

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
    public:
        struct SPriceData
        {
            TieredPriceType mStoragePrice;
            TieredPriceType mClassAOpPrice;
            TieredPriceType mClassBOpPrice;
        };

        using CStorageElement::CStorageElement;

        virtual void OnOperation(OPERATION op) final;

        virtual void OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now) final;
        virtual void RemoveReplica(SReplica* replica, TickType now, bool needLock = true) final;

        auto CalculateStorageCosts(TickType now) -> double;
        auto CalculateOperationCosts() -> double;

        auto GetCurStoragePrice() const -> long double;

        inline auto GetNumClassA() const -> std::size_t
        {return mCostTracking->mNumClassA;}
        inline auto GetNumClassB() const -> std::size_t
        {return mCostTracking->mNumClassB;}


        std::unique_ptr<SPriceData> mPriceData = std::make_unique<SPriceData>();

    private:
        TickType mTimeLastCostUpdate = 0;
        struct SCostTracking
        {
            double mStorageCosts = 0;
            std::size_t mNumClassA = 0;
            std::size_t mNumClassB = 0;
        };

        std::unique_ptr<SCostTracking> mCostTracking = std::make_unique<SCostTracking>();
    };

    class CRegion : public ISite
    {
    public:
        using ISite::ISite;

        auto CreateStorageElement(std::string&& name, bool allowDuplicateReplicas = false, SpaceType limit = 0) -> CBucket* final;
        auto GetStorageElements() const -> std::vector<CStorageElement*> final;

        auto CalculateStorageCosts(TickType now) -> double;
        auto CalculateOperationCosts(std::size_t& numClassA, std::size_t& numClassB) -> double;
        auto CalculateNetworkCosts(double& sumUsedTraffic, std::uint64_t& sumDoneTransfers) -> double;


        std::vector<std::unique_ptr<CBucket>> mStorageElements;
        std::unordered_map<IdType, TieredPriceType> mNetworkLinkIdToPrice;
    };

    class CCloud final : public IBaseCloud
    {
    public:
        using IBaseCloud::IBaseCloud;

        virtual ~CCloud();

        auto CreateRegion(std::string&& name,
                          std::string&& locationName,
                          std::uint8_t multiLocationIdx) -> CRegion* final;

        auto ProcessBilling(const TickType now) -> std::unique_ptr<ICloudBill> final;
        void InitialiseNetworkLinks() final;

        bool LoadConfig(const json& config) final;

    private:
        std::unique_ptr<json> mSKUSettings;
        std::unique_ptr<json> mNetworkPrices;

        auto GetTieredRateFromSKUId(const std::string& skuId) const -> TieredPriceType;
    };

    class CCloudFactory : public ICloudFactory
    {
    private:
        static ICloudFactory* mInstance;

    public:
        CCloudFactory();
        virtual ~CCloudFactory();

        auto CreateCloud(std::string&& cloudName) const->std::unique_ptr<IBaseCloud> final;
    };
}
