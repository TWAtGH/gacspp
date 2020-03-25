#pragma once

#include <memory>

#include "common/constants.h"

#include "third_party/nlohmann/json_fwd.hpp"

using nlohmann::json;


inline IdType GetNewId()
{
    static IdType id = 0;
    return ++id;
}

class CScopedTimeDiff
{
private:
    TimePointType mStartTime;
    DurationType& mOutVal;
    bool mWillAdd;

public:
    CScopedTimeDiff(DurationType& outVal, bool willAdd=false);

    ~CScopedTimeDiff();
};



class IValueLimiter
{
protected:
    double mLimit;

public:
    static auto CreateFromJson(const json& cfg) -> std::unique_ptr<IValueLimiter>;

    IValueLimiter(double limit);

    virtual auto GetLimited(double value) const -> double = 0;

    auto GetLimitValue() const -> double;
};

class CMinAddLimiter : public IValueLimiter
{
public:
    using IValueLimiter::IValueLimiter;

    auto GetLimited(double value) const -> double override;
};

class CMinClipLimiter : public IValueLimiter
{
public:
    using IValueLimiter::IValueLimiter;

    auto GetLimited(double value) const -> double override;
};

class CMaxModuloLimiter : public IValueLimiter
{
public:
    using IValueLimiter::IValueLimiter;

    auto GetLimited(double value) const -> double override;
};

class CMaxClipLimiter : public IValueLimiter
{
public:
    using IValueLimiter::IValueLimiter;

    auto GetLimited(double value) const -> double override;
};



class IValueGenerator
{
private:
    std::unique_ptr<IValueLimiter> mMinLimiter;
    std::unique_ptr<IValueLimiter> mMaxLimiter;

public:
    static auto CreateFromJson(const json& cfg) -> std::unique_ptr<IValueGenerator>;

    virtual auto GetValue(RNGEngineType& rngEngine) -> double = 0;

    void SetMin(std::unique_ptr<IValueLimiter>&& min);
    void SetMax(std::unique_ptr<IValueLimiter>&& max);

    auto GetBetweenMinMax(double value) const -> double;
    auto GetBetweenMaxMin(double value) const -> double;
};

class CFixedValueGenerator : public IValueGenerator
{
private:
    double mValue;

public:
    CFixedValueGenerator(const double value);

    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};

class CNormalRandomValueGenerator : public IValueGenerator
{
private:
    std::normal_distribution<double> mNormalRNGDistribution;

public:
    CNormalRandomValueGenerator(const double mean, const double stddev);

    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};

class CExponentialRandomValueGenerator : public IValueGenerator
{
private:
    std::exponential_distribution<double> mExponentialRNGDistribution;

public:
    CExponentialRandomValueGenerator(const double lambda);

    virtual auto GetValue(RNGEngineType& rngEngine) -> double override;
};