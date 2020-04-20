#pragma once

#include <memory>

#include "common/constants.h"

struct SFile;
struct SReplica;


class IRucioActionListener
{
public:
    virtual void PostCreateFile(SFile* file, TickType now) = 0;
    virtual void PreRemoveFile(SFile* file, TickType now) = 0;
    //virtual void PreRemoveFilse(TickType now, const std::vector<SFile*>& files) = 0;
};


class IStorageElementActionListener
{
public:
    virtual void PostCreateReplica(SReplica* replica, TickType now) = 0;
    virtual void PreRemoveReplica(SReplica* replica, TickType now) = 0;
    //virtual void PreRemoveReplicas(TickType now, const std::vector<SReplica*>& replica) = 0;
};


class IReplicaPreRemoveListener
{
public:
    virtual bool PreRemoveReplica(SReplica* replica, TickType now) = 0;
};