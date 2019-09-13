#!/usr/bin/env python

import copy
import json
import os.path



def UpdatedSimRegionConf(simRegionConf, gcpData):
    for regionObj in simRegionConf:
        skuId = regionObj.get('skuId')
        gcpRegionData = gcpData.get(skuId)
        assert(skuId != None and gcpRegionData != None)
        pricingInfoList = gcpRegionData.get('pricingInfo', [])
        assert(len(pricingInfoList) == 1)
        pricingInfo = pricingInfoList[0]
        tieredRates = pricingInfo.get('pricingExpression', {}).get('tieredRates', [])
        if len(tieredRates) > 0:
            regionObj['tieredRates'] = tieredRates
            print('Updated {}'.format(regionObj['name']))
        else:
            print('Failed to updated {}'.format(regionObj['name']))


simConf = {}
with open(os.path.join('config', 'gcp_default.json'), 'r') as simConfFile:
    simConf = json.load(simConfFile)

gcpData = {}
with open('skus.json', 'r') as gcpDataFile:
    gcpData = json.load(gcpDataFile)

if len(gcpData.get('skus', [])) > 0:
    bySKUId = {}
    for skuObj in gcpData['skus']:
        skuId = skuObj.get('skuId')
        assert(skuId != None and skuId not in bySKUId)
        bySKUId[skuId] = skuObj
    gcpData = bySKUId
else:
    print('No skus found in skus.json')

simRegionConf = simConf.get('gcp', {}).get('regions', {})
if len(simRegionConf) > 0:
    UpdatedSimRegionConf(simRegionConf, gcpData)
    with open(os.path.join('config', 'new_gcp_default.json'), 'w') as newSimConfFile:
        json.dump(simConf, newSimConfFile, indent=2)
else:
    print('No regions found in gcp_default.json')
