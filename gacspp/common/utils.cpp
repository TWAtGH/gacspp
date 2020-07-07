#include <cassert>

#include "utils.hpp"

#include "third_party/nlohmann/json.hpp"


CScopedTimeDiffSet::CScopedTimeDiffSet(DurationType& outVal)
    : mStartTime(std::chrono::high_resolution_clock::now()),
      mOutVal(outVal)
{}

CScopedTimeDiffSet::~CScopedTimeDiffSet()
{
    mOutVal = std::chrono::high_resolution_clock::now() - mStartTime;
}

CScopedTimeDiffAdd::CScopedTimeDiffAdd(DurationType& outVal)
    : mStartTime(std::chrono::high_resolution_clock::now()),
      mOutVal(outVal)
{}

CScopedTimeDiffAdd::~CScopedTimeDiffAdd()
{
    mOutVal += std::chrono::high_resolution_clock::now() - mStartTime;
}



auto IValueLimiter::CreateFromJson(const json& cfg) -> std::unique_ptr<IValueLimiter>
{
    std::unique_ptr<IValueLimiter> valueLimiter;
    const std::string type = cfg.at("type").get<std::string>();
    double limit = cfg.at("limit").get<double>();
    bool invert = false;
    if (cfg.contains("invert"))
        cfg.at("invert").get_to(invert);

    if(type == "minAdd")
        valueLimiter = std::make_unique<CMinAddLimiter>(limit);
    if(type == "minClip")
        valueLimiter = std::make_unique<CMinClipLimiter>(limit);
    else if(type == "maxModulo")
        valueLimiter = std::make_unique<CMaxModuloLimiter>(limit, invert);
    else if(type == "maxClip")
        valueLimiter = std::make_unique<CMaxClipLimiter>(limit, invert);

    assert(valueLimiter);

    return valueLimiter;
}

IValueLimiter::IValueLimiter(double limit, bool invert)
    : mLimit(limit),
      mInvert(invert)
{}

auto IValueLimiter::GetLimitValue() const -> double
{
    return mLimit;
}

auto CMinAddLimiter::GetLimited(double value) const -> double
{
    return (mLimit + std::abs(value));
}


auto CMinClipLimiter::GetLimited(double value) const -> double
{
    return ((value < mLimit) ? mLimit : value);
}

auto CMaxModuloLimiter::GetLimited(double value) const -> double
{
    if(value > mLimit)
        value = static_cast<double>(static_cast<std::uint64_t>(value) % static_cast<std::uint64_t>(mLimit));

    if (mInvert)
        return mLimit - value;
    return value;
}

auto CMaxClipLimiter::GetLimited(double value) const -> double
{
    if (value > mLimit)
        value = mLimit;

    if (mInvert)
        return mLimit - value;
    return value;
}



auto IValueGenerator::CreateFromJson(const json& cfg) -> std::unique_ptr<IValueGenerator>
{
    std::unique_ptr<IValueGenerator> valueGenerator;
    const std::string type = cfg.at("type").get<std::string>();
    if(type == "normal")
    {
        const double mean = cfg.at("mean");
        const double stddev = cfg.at("stddev");
        valueGenerator = std::make_unique<CNormalRandomValueGenerator>(mean, stddev);
    }
    else if(type == "exponential")
    {
        const double lambda = cfg.at("lambda");
        valueGenerator = std::make_unique<CExponentialRandomValueGenerator>(lambda);
    }
    else if (type == "geometric")
    {
        const double p = cfg.at("p");
        valueGenerator = std::make_unique<CExponentialRandomValueGenerator>(p);
    }
    else if(type == "fixed")
    {
        const double value = cfg.at("value");
        valueGenerator = std::make_unique<CFixedValueGenerator>(value);
    }

    assert(valueGenerator);

    if(cfg.count("minCfg"))
        valueGenerator->SetMin(IValueLimiter::CreateFromJson(cfg.at("minCfg")));
    if(cfg.count("maxCfg"))
        valueGenerator->SetMax(IValueLimiter::CreateFromJson(cfg.at("maxCfg")));

    return valueGenerator;
}

void IValueGenerator::SetMin(std::unique_ptr<IValueLimiter>&& min)
{
    assert(min);
    if (mMaxLimiter)
    {
        assert(min->GetLimitValue() < mMaxLimiter->GetLimitValue());
    }
    mMinLimiter = std::move(min);
}

void IValueGenerator::SetMax(std::unique_ptr<IValueLimiter>&& max)
{
    assert(max);
    if (mMinLimiter)
    {
        assert(max->GetLimitValue() > mMinLimiter->GetLimitValue());
    }
    mMaxLimiter = std::move(max);
}

auto IValueGenerator::GetBetweenMinMax(double value) const -> double
{
    if(mMinLimiter)
        value = mMinLimiter->GetLimited(value);
    if(mMaxLimiter)
        value = mMaxLimiter->GetLimited(value);
    return value;
}

auto IValueGenerator::GetBetweenMaxMin(double value) const -> double
{
    if(mMaxLimiter)
        value = mMaxLimiter->GetLimited(value);
    if(mMinLimiter)
        value = mMinLimiter->GetLimited(value);
    return value;
}

CFixedValueGenerator::CFixedValueGenerator(const double value)
    : mValue(value)
{}

auto CFixedValueGenerator::GetValue(RNGEngineType& rngEngine) -> double
{
    (void)rngEngine;
    return GetBetweenMinMax(mValue);
}

CNormalRandomValueGenerator::CNormalRandomValueGenerator(const double mean, const double stddev)
    : mNormalRNGDistribution(mean, stddev)
{}

auto CNormalRandomValueGenerator::GetValue(RNGEngineType& rngEngine) -> double
{
    return GetBetweenMaxMin(mNormalRNGDistribution(rngEngine));
}

CExponentialRandomValueGenerator::CExponentialRandomValueGenerator(const double lambda)
    : mExponentialRNGDistribution(lambda)
{
    assert(lambda > 0);
}

auto CExponentialRandomValueGenerator::GetValue(RNGEngineType& rngEngine) -> double
{
    return GetBetweenMaxMin(mExponentialRNGDistribution(rngEngine));
}

CGeometricRandomValueGenerator::CGeometricRandomValueGenerator(const double p)
    : mGeometricRNGDistribution(p)
{
    assert((0 < p) && (p < 1));
}

auto CGeometricRandomValueGenerator::GetValue(RNGEngineType& rngEngine) -> double
{
    return GetBetweenMaxMin(mGeometricRNGDistribution(rngEngine));
}
