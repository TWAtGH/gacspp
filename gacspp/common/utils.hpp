/**
 * @file   utils.hpp
 * @brief  Provides a various util functions and helper classes
 *
 * @author Tobias Wegner
 * @date   March 2022
 */

#pragma once

#include <memory>

#include "common/constants.h"

#include "third_party/nlohmann/json_fwd.hpp"

using nlohmann::json;

/**
* @brief Generates and returns a new unique ID
* 
* @return newly generated ID
*/
inline IdType GetNewId()
{
    static IdType id = 0;
    return ++id;
}


/**
* @brief Helper class to measure the real time that was spent in a certain scope
* 
* The class measures the time it was constructed and destructed at. The difference
* is stored in the variable referenced by the passed in reference.
*/
class CScopedTimeDiffSet
{
private:
    /**
    * @brief The time point in real time the object was constructed at
    */
    TimePointType mStartTime;

    /**
    * @brief Reference where the measured time difference will be stored in
    */
    DurationType& mOutVal;

public:
    CScopedTimeDiffSet(DurationType& outVal);

    ~CScopedTimeDiffSet();
};


/**
* @brief Helper class to measure the real time that was spent in a certain scope
*
* The class measures the time it was constructed and destructed at. The difference
* is added to the variable referenced by the passed in reference. Similar to CScopedTimeDiffSet
* but the difference will be added to the output variable instead of overwriting it.
*/
class CScopedTimeDiffAdd
{
private:
    /**
    * @brief The time point in real time the object was constructed at
    */
    TimePointType mStartTime;

    /**
    * @brief Reference where the measured time difference will be added to
    */
    DurationType& mOutVal;

public:
    CScopedTimeDiffAdd(DurationType& outVal);

    ~CScopedTimeDiffAdd();
};


/**
* @brief Interface that generalises the limitation of a given value.
*/
class IValueLimiter
{
protected:
    /**
    * @brief The limit to use
    */
    double mLimit;

    /**
    * @brief Should the value be inverted?
    */
    bool mInvert;

public:

    /**
    * @brief Static helper method to create a known implementation from a given json object
    * 
    * @param cfg the json object desribing the value limiter (type and parameters)
    * 
    * @return a unique pointer of the requested IValueLimiter implementation or nullptr if invalid config was given
    */
    static auto CreateFromJson(const json& cfg) -> std::unique_ptr<IValueLimiter>;


    /**
    * @brief Initialises the limiter from the given values
    * 
    * @param limit the limit to apply
    * @param invert should the input value be inverted?
    */
    IValueLimiter(double limit, bool invert = false);

    /**
    * @brief Receives the a value and returns the potentially limited value
    * 
    * @param value the input value
    * 
    * @return the potentially limited value
    */
    virtual auto GetLimited(double value) const -> double = 0;

    /**
    * @brief Getter for the configured limit
    * 
    * @return the configured limit
    */
    auto GetLimitValue() const -> double;
};

/**
* @brief Implementation that applies a minimum limit by adding the minimum to the input value
*/
class CMinAddLimiter : public IValueLimiter
{
public:
    using IValueLimiter::IValueLimiter;

    /**
    * @brief This implementation adds the limit to value and returns the result.
    * 
    * @param value the input value
    * 
    * @return the sum of the limit and input value
    * 
    * Forces the input value to be positive.
    */
    auto GetLimited(double value) const -> double override;
};

/**
* @brief Implementation that applies a minimum limit by clamping the input value to the limit
*/
class CMinClipLimiter : public IValueLimiter
{
public:
    using IValueLimiter::IValueLimiter;

    /**
    * @brief This implementation returns the greater of the limit and the input value.
    *
    * @param value the input value
    *
    * @return the maximum of the limit value and the input value
    */
    auto GetLimited(double value) const -> double override;
};

/**
* @brief Implementation that applies a maximum limit by using the modulo operator
*/
class CMaxModuloLimiter : public IValueLimiter
{
public:
    using IValueLimiter::IValueLimiter;

    /**
    * @brief This implementation limits the input value using modulo.
    *
    * @param value the input value
    *
    * @return if the input value is greater than the limit the modulo result is returned. Otherwise the input value is returned
    */
    auto GetLimited(double value) const -> double override;
};

/**
* @brief Implementation that applies a maximum limit by clamping the value to the limit
*/
class CMaxClipLimiter : public IValueLimiter
{
public:
    using IValueLimiter::IValueLimiter;

    /**
    * @brief This implementation returns the lower of the limit and the input value.
    *
    * @param value the input value
    *
    * @return the minimum of the limit and the input value
    *
    * Forces the input value to be positive.
    */
    auto GetLimited(double value) const -> double override;
};



/**
* @brief Interface that generalises the generation of values
* 
* Typically, used to generate exchangeable random distributions using a unified type.
* Optionally, a min and max limiter can be configured to set the limits.
*/
class IValueGenerator
{
private:
    std::unique_ptr<IValueLimiter> mMinLimiter;
    std::unique_ptr<IValueLimiter> mMaxLimiter;

public:

    /**
    * @brief Static helper method to create a known implementation from a given json object
    *
    * @param cfg the json object desribing the value generator (type and parameters)
    *
    * @return a unique pointer of the requested IValueGenerator implementation or nullptr if invalid config was given
    */
    static auto CreateFromJson(const json& cfg) -> std::unique_ptr<IValueGenerator>;

    /**
    * @brief Generates a value
    * 
    * @param rngEngine random number generation engine
    * 
    * @return the generated value
    */
    virtual auto GetValue(RNGEngineType& rngEngine) -> double = 0;

    /**
    * @brief Setter for the minimum value limiter
    * 
    * @param min minimum value limiter instance (will be consumed)
    */
    void SetMin(std::unique_ptr<IValueLimiter>&& min);

    /**
    * @brief Setter for the maximum value limiter
    *
    * @param max maximum value limiter instance (will be consumed)
    */
    void SetMax(std::unique_ptr<IValueLimiter>&& max);

    /**
    * @brief Applies first the min limiter and then the max limiter to the given value
    *
    * @param value input value to pass to the limiters
    * 
    * @return the limited value
    */
    auto GetBetweenMinMax(double value) const -> double;

    /**
    * @brief Applies first the max limiter and then the min limiter to the given value
    *
    * @param value input value to pass to the limiters
    *
    * @return the limited value
    */
    auto GetBetweenMaxMin(double value) const -> double;
};

/**
* @brief Implementation that always returns the configured value
*/
class CFixedValueGenerator : public IValueGenerator
{
private:
    /**
    * @brief The fixed value to return by GetValue()
    */
    double mValue;

public:
    /**
    * @brief Initialises the object
    * 
    * @param value the value that this generator will deliver
    */
    CFixedValueGenerator(const double value);

    /**
    * @brief Returns the configured value
    *
    * @param rngEngine random number generation engine
    *
    * @return mValue
    */
    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};

/**
* @brief Implementation that generates normally distributed random numbers
*/
class CNormalRandomValueGenerator : public IValueGenerator
{
private:
    /**
    * @brief Object describing and generating the normal distribution
    */
    std::normal_distribution<double> mNormalRNGDistribution;

public:
    /**
    * @brief Initialises the object
    *
    * @param mean the mean of the normal distribution
    * @param stddev the standard deviation of the normal distribution
    */
    CNormalRandomValueGenerator(const double mean, const double stddev);

    /**
    * @brief Generates a normally distributed random value
    *
    * @param rngEngine random number generation engine
    *
    * @return the generated random value
    */
    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};

/**
* @brief Implementation that generates exponentially distributed random numbers
*/
class CExponentialRandomValueGenerator : public IValueGenerator
{
private:
    /**
    * @brief Object describing and generating the exponential distribution
    */
    std::exponential_distribution<double> mExponentialRNGDistribution;

public:
    /**
    * @brief Initialises the object
    *
    * @param lambda the lambda paramter of the exponential distribution
    * 
    * mean = 1/lambda
    */
    CExponentialRandomValueGenerator(const double lambda);

    /**
    * @brief Generates an exponentially distributed random value
    *
    * @param rngEngine random number generation engine
    *
    * @return the generated random value
    */
    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};

/**
* @brief Implementation that generates poisson distributed random numbers
*/
class CPoissonRandomValueGenerator : public IValueGenerator
{
private:
    /**
    * @brief Object describing and generating the poisson distribution
    */
    std::poisson_distribution<int> mPoissonRNGDistribution;

public:
    /**
    * @brief Initialises the object
    *
    * @param mean the mean of the poisson distribution
    */
    CPoissonRandomValueGenerator(const double mean);

    /**
    * @brief Generates a poisson distributed random value
    *
    * @param rngEngine random number generation engine
    *
    * @return the generated random value
    */
    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};

/**
* @brief Implementation that generates weibull distributed random numbers
*/
class CWeibullRandomValueGenerator : public IValueGenerator
{
private:
    /**
    * @brief Object describing and generating the weibull distribution
    */
    std::weibull_distribution<double> mWeibullRNGDistribution;

public:
    /**
    * @brief Initialises the object
    *
    * @param k the k factor of the distribution (shape parameter)
    * @param lambda the scale factor of the distribution
    */
    CWeibullRandomValueGenerator(const double k, const double lambda = 1.0);

    /**
    * @brief Generates a weibull distributed random value
    *
    * @param rngEngine random number generation engine
    *
    * @return the generated random value
    */
    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};

/**
* @brief Implementation that generates exponential weibull distributed random numbers
*/
class CExponentiatedWeibullRandomValueGenerator : public IValueGenerator
{
private:
    /**
    * @brief exponentation parameter
    */
    double mA = 1;

    /**
    * @brief shape parameter
    */
    double mC = 1;

    /**
    * @brief scale factor
    */
    double mL = 1;

public:
    /**
    * @brief Initialises the object
    *
    * @param a the exponentation parameter
    * @param k the shape parameter of the distribution
    * @param l the scale factor of the distribution
    * 
    * It is required that: a > 0, k > 0, and l > 0
    * a = 1 results in the standard weibull distribution
    */
    CExponentiatedWeibullRandomValueGenerator(const double a, const double c, const double l);

    /**
    * @brief Generates an exponential weibull distributed random value
    *
    * @param rngEngine random number generation engine
    *
    * @return the generated random value
    */
    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};

/**
* @brief Implementation that generates geometrically distributed random numbers
*/
class CGeometricRandomValueGenerator : public IValueGenerator
{
private:
    /**
    * @brief Object describing and generating the geometric distribution
    */
    std::geometric_distribution<std::uint64_t> mGeometricRNGDistribution;

public:
    /**
    * @brief Initialises the object
    *
    * @param p probability of a trial generating true
    */
    CGeometricRandomValueGenerator(const double p);

    /**
    * @brief Generates a geometrically distributed random value
    *
    * @param rngEngine random number generation engine
    *
    * @return the generated random value
    */
    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};