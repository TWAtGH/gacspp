#pragma once

#include <memory>

#include "common/constants.h"

struct SFile;
struct SReplica;


class IRucioActionListener
{
public:
    virtual ~IRucioActionListener() = default;
    virtual void PostCreateFile(SFile* file, TickType now) = 0;
    virtual void PreRemoveFile(SFile* file, TickType now) = 0;
};


class IStorageElementActionListener
{
public:
    virtual ~IStorageElementActionListener() = default;
    virtual void PostCompleteReplica(SReplica* replica, TickType now) = 0;
    virtual void PostCreateReplica(SReplica* replica, TickType now) = 0;
    virtual void PreRemoveReplica(SReplica* replica, TickType now) = 0;
};


class IReplicaPreRemoveListener
{
public:
    virtual ~IReplicaPreRemoveListener() = default;
    virtual bool PreRemoveReplica(SReplica* replica, TickType now) = 0;
};