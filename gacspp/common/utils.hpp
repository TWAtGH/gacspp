#pragma once

#include "common/constants.h"

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
    CScopedTimeDiff(DurationType& outVal, bool willAdd=false)
        : mStartTime(std::chrono::high_resolution_clock::now()),
          mOutVal(outVal),
          mWillAdd(willAdd)
    {}

    ~CScopedTimeDiff()
    {
        const auto duration = std::chrono::high_resolution_clock::now() - mStartTime;
        if (mWillAdd)
            mOutVal += duration;
        else
            mOutVal = duration;
    }
};

class IValueGenerator
{
public:
    virtual auto GetValue(RNGEngineType& rngEngine) -> double = 0;
};

class CFixedValueGenerator : public IValueGenerator
{
private:
    double mValue;

public:
    CFixedValueGenerator(const double value)
        : mValue(value)
    {}

    virtual auto GetValue(RNGEngineType& rngEngine) -> double
    {
        (void)rngEngine;
        return mValue;
    }
};

class CNormalRandomValueGenerator : public IValueGenerator
{
private:
    std::normal_distribution<double> mNormalRNGDistribution;

public:
    CNormalRandomValueGenerator(const double mean, const double stddev)
        : mNormalRNGDistribution(mean, stddev)
    {}

    virtual auto GetValue(RNGEngineType& rngEngine) -> double
    {
        return mNormalRNGDistribution(rngEngine);
    }
};