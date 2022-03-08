/**
 * @file   CCloudGCP.hpp
 * @brief  Contains the cloud interface implementation for GCP
 *
 * @author Tobias Wegner
 * @date   March 2022
 * 
 * This file contains the implementation of the cloud interfaces for the Google Cloud Platform (GCP).
 * The cloud is provided by implementing the required interfaces: ICloudBill, IBaseCloud, and ICloudFactory.
 * Furthermore, a custom CStorageElement and ISite implementation is used to represent GCP functionality.
 */
#pragma once

#include "clouds/IBaseCloud.hpp"

#include "infrastructure/ISite.hpp"
#include "infrastructure/CStorageElement.hpp"


namespace gcp
{
    class CRegion;
    typedef std::vector<std::pair<std::uint32_t, long double>> TieredPriceType;

    /**
    * @brief GCP implementation of the ICloudBill interface
    * 
    * A bill for GCP consists mainly of the storage, network, and operation costs. These costs will be calcluated during
    * the bill calculation in the cloud instance and then be stored in the corresponding CCloudBill member variables.
    * Network cost are based on the used traffic. The traffic is also stored in a member variable and can be outputed.
    * The operation cost consists of class A and class B operations. The numbers of each operation are additonally stored
    * in corresponding member variables.
    */
    class CCloudBill : public ICloudBill
    {
    private:
        /** Storage cost */
        double mStorageCost;

        /** Network cost */
        double mNetworkCost;

        /** Induced traffic */
        double mTraffic;

        /** Operation cost */
        double mOperationCost;

        /** Number of class A operations */
        std::size_t mNumClassA;

        /** Number of class B operations */
        std::size_t mNumClassB;

    public:

        /**
         * @brief Constructor to create a GCP bill given the required data. This will be called inside the ProcessBilling method of the GCP cloud implementation.
        */
        CCloudBill(double storageCost, double networkCost, double traffic, double operationCost, std::size_t numClassA, std::size_t numClassB);

        /**
         * @brief Converts the GCP bill into a string representation
         *
         * @return string representing the GCP bill
         */
        virtual std::string ToString() const final;
    };


    /** 
    * @brief CStorageElement specification to represent a GCP bucket.
    * 
    * This class overrrides certain functionalities to allow keeping track of the introduced costs.
    * Furthermore, functionality is added to allow the cloud processing the cost.
    * Typically, CBucket objects are created by their owning CRegion object. The entity that creates
    * the bucket is responsible for setting proper pricing information.
    */
    class CBucket : public CStorageElement
    {
    public:

        /**
        * @brief SPriceData provides an extra encapsulation of the pricing information.
        */
        struct SPriceData
        {
            TieredPriceType mStoragePrice;
            TieredPriceType mClassAOpPrice;
            TieredPriceType mClassBOpPrice;
        };

        using CStorageElement::CStorageElement;

        /**
         * @brief This function is overridden by the GCP implementation to take note of the occurred storage operations to calculate costs.
         * 
         * @param op operation that is executed on the bucket, e.g., read, write, delete
         */
        virtual void OnOperation(OPERATION op) final;

        /**
        * @brief Function is overridden to add track-keeping of the increase of storage space to allow precise cost calculation.
        *
        * @param replica valid pointer to the replica that was increased
        * @param amount the amount that the replica was increased by
        * @param now the time at which the replica was increased
        */
        virtual void OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now) final;

        /**
        * @brief Function is overridden to add track-keeping of the decrease of storage space to allow precise cost calculation.
        *
        * @param replica valid pointer to the replica that should be removed. Asserts if the given replica belongs to a different storage element
        * @param now the simulation time the replica was removed on
        * @param needLock uses a mutex to lock protect the internal data structures if deletion is done in different threads
        */
        virtual void RemoveReplica(SReplica* replica, TickType now, bool needLock = true) final;

        /**
        * @brief Calculates the cost introduced by using storage space
        *
        * @param now the current simulation time
        * 
        * @return the cost for the usage of cloud storage
        * 
        * The storage space cost is calculated from the last time the function was called up to the passed in simulation time point. This means
        * the cost will be reset during this function.
        */
        auto CalculateStorageCosts(TickType now) -> double;


        /**
        * @brief Calculates the cost introduced by operations on this bucket
        *
        * @return the cost for executed operations ( OnOperation() )
        * 
        * The cost calculation does not consider operations prior to the last time this function was called. In other words, the function resets
        * the number of executed operations.
        */
        auto CalculateOperationCosts() -> double;


        /**
        * @brief Helper function to get the storage price considering the currently used storage
        *
        * @return the price in USD per GiB per month, considering the currently used amount of storage space
        *
        * The pricing details for many regions in GCP are layered. For example, storing 10 GiB of data cost x USD per GiB per month.
        * Storing more than 10 GiB but less than 100 GiB of data cost y USD per GiB per month. This function returns the price value
        * considering the currently used amount of storage.
        */
        auto GetCurStoragePrice() const -> long double;

        inline auto GetNumClassA() const -> std::size_t
        {return mCostTracking->mNumClassA;}
        inline auto GetNumClassB() const -> std::size_t
        {return mCostTracking->mNumClassB;}


        /**
        * @brief SPriceData object storing the pricing information. Must be initialised after creating the bucket.
        */
        std::unique_ptr<SPriceData> mPriceData = std::make_unique<SPriceData>();

    private:
        /**
        * @brief Each time the used storage changes, the current cost must be updated.
                 This variable stores the most recent time the cost was updated.
        */
        TickType mTimeLastCostUpdate = 0;


        /**
        * @brief An additional encapsulation for storing the current calculated cost. Internal use only.
        */
        struct SCostTracking
        {

            /**
            * @brief Current storage cost
            */
            double mStorageCosts = 0;

            /**
            * @brief Current number of class A operations
            */
            std::size_t mNumClassA = 0;

            /**
            * @brief Current number of class B operations
            */
            std::size_t mNumClassB = 0;
        };

        /**
        * @brief Internal data for tracking the cost
        */
        std::unique_ptr<SCostTracking> mCostTracking = std::make_unique<SCostTracking>();
    };


    /**
    * @brief GCP implementation of the ISite interface to generate site objects that represent GCP regions.
    *
    * This class implements the ISite methods to allow generating and storing gcp::CBucket objects.
    * Furthermore, it provides additional methods to calculate and aggregate of all buckets owned by this region.
    */
    class CRegion : public ISite
    {
    public:
        using ISite::ISite;

        /**
        * @brief GCP specific implementation that creates instances gcp::CBucket
        *
        * @param name name of the new bucket (string will be consumed)
        * @param allowDuplicateReplicas whether the created bucket is allowed to store only unique replicas or duplicates as well
        * @param limit storage limit of the new bucket
        *
        * @return a pointer to the newly created bucket object. The pointer is valid as long as the GCP region is valid
        */
        auto CreateStorageElement(std::string&& name, bool allowDuplicateReplicas = false, SpaceType limit = 0) -> CBucket* final;

        /**
        * @brief Generates an array containing pointers to all associated buckets
        *
        * @return array with a pointer for each of the owned buckets
        */
        auto GetStorageElements() const -> std::vector<CStorageElement*> final;


        /**
        * @brief Calculates the storage cost of this region. Triggers and aggregates storage cost calculation of each owned bucket.
        *
        * @param now current simulation time
        * 
        * @return the total cost for the storage space used at this region
        */
        auto CalculateStorageCosts(TickType now) -> double;

        /**
        * @brief Calculates the operation cost of this region. Triggers and aggregates operation cost calculation of each owned bucket.
        *
        * @param numClassA OUT: contains the total number of class A operations for this region after the method returns
        * @param numClassB OUT: contains the total number of class B operations for this region after the method returns
        *
        * @return the total cost for the operations executed at this region
        */
        auto CalculateOperationCosts(std::size_t& numClassA, std::size_t& numClassB) -> double;

        /**
        * @brief Calculates the network cost of this region.
        *
        * @param sumUsedTraffic OUT: contains the total outbound traffic of this region after the method returns
        * @param sumDoneTransfers OUT: contains the total number of finished transfers going out from this region after the method returns
        *
        * @return the total cost for the outbound traffic of this region
        */
        auto CalculateNetworkCosts(double& sumUsedTraffic, std::uint64_t& sumDoneTransfers) -> double;


        /**
        * @brief Array containing all buckets owned by this region.
        */
        std::vector<std::unique_ptr<CBucket>> mStorageElements;

        /**
        * @brief Allows fast lookup of the pricing information given a network link id. Used for network cost calculation.
        */
        std::unordered_map<IdType, TieredPriceType> mNetworkLinkIdToPrice;
    };


    /**
    * @brief IBaseCloud implementation that implements the Google Cloud Platform (GCP)
    * 
    * This implementation allows creating the gcp::CRegion objects using the CreateRegion() method. The ProcessBilling() method
    * is implemented using the region objects associated with this cloud and returning a gcp::CCloudBill object.
    * In addition, the LoadConfig method is called by the simulation engine if a config file is associated with the cloudId.
    */
    class CCloud final : public IBaseCloud
    {
    public:
        using IBaseCloud::IBaseCloud;

        virtual ~CCloud();

        /**
        * @brief Method to create a grid site. Implemented by creating CGridSite objects.
        *
        * @param name name of the new cloud region (string will be consumed)
        * @param locationName geographical location name of the cloud region
        * @param multiLocationIdx index of the top level location if the region is multi regional
        *
        * @return a pointer to a gcp::CRegion object that will be valid as long as this cloud object is valid
        */
        auto CreateRegion(std::string&& name,
                          std::string&& locationName,
                          std::uint8_t multiLocationIdx) -> CRegion* final;

        /**
        * @brief Method to calculate the cost that this cloud introduced since the last time this method was called.
        *
        * @param now current simulation time stamp
        *
        * @return a unique pointer to an ICloudBill instance, which is implemented by the gcp::CCloudBill class.
        * 
        * Internally, the method iterates through all regions calling their corresponding cost calculation methods.
        */
        auto ProcessBilling(const TickType now) -> std::unique_ptr<ICloudBill> final;


        /**
        * @brief This method sets the pricing information for the network links of each region (gcp::CRegion::mNetworkLinkIdToPrice)
        */
        void InitialiseNetworkLinks() final;

        /**
        * @brief Applies the configuration passed in to the cloud instance
        *
        * @param config the json object that contains all settings to use for this cloud instance
        *
        * @return true if initialisation was successfull, false otherwise
        * 
        * The GCP implementation of this function basically works in two steps. First, it loads the pricing information from the
        * json object. Second, it creates all regions and sites as configured in the json object and sets their pricing details
        * using the information loaded in the first step.
        */
        bool LoadConfig(const json& config) final;

    private:
        /**
        * @brief SKU ids are used by GCP to identify service, such as operation types, traffic, or storage types.
        * This variable allows mapping those services to their prices. Used only internally.
        */
        std::unique_ptr<json> mSKUSettings;

        /**
        * @brief Stores network price information that are initialised during the first step of the LoadConfig method.
        * Used only internally.
        */
        std::unique_ptr<json> mNetworkPrices;

        /**
        * @brief Internal helper function to get pricing info from an SKU ID
        * 
        * @param skuId the desired SKU ID
        * 
        * return the tiered pricing information
        * 
        * The pricing details for many regions in GCP are layered. For example, storing 10 GiB of data cost x USD per GiB per month.
        * Storing more than 10 GiB but less than 100 GiB of data cost y USD per GiB per month. This function returns all layers.
        */
        auto GetTieredRateFromSKUId(const std::string& skuId) const -> TieredPriceType;
    };


    /**
    * @brief ICloudFactory implementation that implements a cloud factory to create GCP cloud objects
    *
    * The class is implemented as singleton. The static member variable mInstance is initialised with an object.
    * The constructor will register itself at the CCloudFactoryManager.
    */
    class CCloudFactory : public ICloudFactory
    {
    private:
        static ICloudFactory* mInstance;

    public:
        CCloudFactory();
        virtual ~CCloudFactory();

        /**
         * @brief Create a new cloud instance of the GCP implementation using the provided name
         *
         * @param cloudName name of the newly created cloud (string will be consumed)
         *
         * @return A unique pointer of the newly created cloud
         */
        auto CreateCloud(std::string&& cloudName) const->std::unique_ptr<IBaseCloud> final;
    };
}
