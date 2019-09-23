#!/usr/bin/env python

import argparse
import os
import shutil
import statistics

import matplotlib.pyplot as plt



def NullPlot(*args):
    pass


def PlotTransferMgr(filePath, plotOutputFilePath=None):
    plt.figure(num=filePath, figsize=(25, 10), dpi=200)
    plt.ticklabel_format(axis='both', style='plain')
    plt.grid(axis='y')
    with open(filePath, 'r') as infile:
        data = infile.read().split('|')
    data.pop()
    items = (([],[]), ([],[]))
    print('lenData={}'.format(len(data)))
    for idx in range(0, len(data), 3):
        eventId = int(data[idx])
        items[eventId][0].append(int(data[idx+1])/1000)
        items[eventId][1].append(int(data[idx+2]))
    plt.plot(items[0][0], items[0][1], label='NumActiveTransfers')
    plt.plot(items[1][0], items[1][1], label='NumTransfersToCreate')
    plt.xlabel('Sim Time/1000')
    plt.ylabel('Count')
    plt.legend()
    plt.axis(xmin=0, ymin=0)
    if plotOutputFilePath:
        plt.savefig('{}.png'.format(plotOutputFilePath))
        plt.savefig('{}.svg'.format(plotOutputFilePath))


def PlotBilling(filePath, plotOutputFilePath=None):
    plt.figure(num=filePath, figsize=(25, 10), dpi=200)
    plt.ticklabel_format(axis='both', style='plain')
    plt.grid(axis='y')
    bucketsById = {}
    with open(filePath, "r") as infile:
        for line in infile:
            line = line.rstrip()
            if line == 'body':
                break
            items = line.split(':')
            bucketsById[items[0]] = {'name': items[1], 'x': [], 'y': []}

        data = infile.read().split('|')
        data.pop()

    print('lenData={}'.format(len(data)))
    for idx in range(0, len(data)-3, 3):
        bucket = bucketsById[data[idx]]
        bucket['x'].append( int(data[idx+1])/1000 )
        bucket['y'].append( float(data[idx+2]) )

    for id in bucketsById:
        bucket = bucketsById[id]
        plt.plot(bucket['x'], bucket['y'], label=bucket['name'])

    plt.xlabel('Sim Time/1000')
    plt.ylabel('Storage Volume [GiB]')
    plt.legend()
    plt.axis(xmin=0, ymin=0)
    if plotOutputFilePath:
        plt.savefig('{}.png'.format(plotOutputFilePath))
        plt.savefig('{}.svg'.format(plotOutputFilePath))


def PlotTrafficDiff(simFilePath, refFilePath, startingSecond, plotOutputFilePath=None):
    plt.figure(num=simFilePath, figsize=(25, 10), dpi=200)
    plt.ticklabel_format(axis='both', style='plain')
    plt.grid(axis='y')

    scale = 2**30
    with open(simFilePath, 'r') as infile:
        data = infile.read().split('|')
    data.pop()
    print('lenData={}'.format(len(data)))
    simY = [0]
    intervalEnd = startingSecond + 3600*3
    for idx in range(0, len(data), 2):
        now = int(data[idx])
        if now < startingSecond:
            continue
        if now  >= intervalEnd:
            simY.append(simY[-1])
            intervalEnd += 3600*3
        simY[-1] += int(int(data[idx+1]) / scale)

    plt.plot(range(0, intervalEnd-startingSecond, 3600*3), simY, label='SummedSimTraffic')

    refX = [0]
    refY = []
    with open(refFilePath, 'r') as infile:
        # dump first line
        data = infile.readline()
        data = infile.readline().strip().split(',')
        baseX = int(int(data[0]) / 1000)
        refY.append(int(data[1]) / scale)
        for line in infile.readlines():
            data = line.strip().split(',')
            x = int(int(data[0]) / 1000)
            refX.append(x - baseX)
            refY.append(int(int(data[1]) / scale))
    print('lenData={}'.format(len(refY)))
    plt.plot(refX, refY, label='SummedRefTraffic')

    plt.xlabel('Sim Time')
    plt.ylabel('Sum of transferred bytes/GiB')
    plt.legend()
    plt.axis(xmin=0, ymin=0)
    if plotOutputFilePath:
        plt.savefig('{}.png'.format(plotOutputFilePath))
        plt.savefig('{}.svg'.format(plotOutputFilePath))


def SimplePlotHandler(options, argsDict):
    #options[0][0]: file path option key
    #options[1][0]: plotting function option key
    srcFileName = argsDict.get(options[0][0].replace('-', '_'))
    srcFilePath = os.path.join(os.path.abspath(args.InputBasePath), srcFileName)
    if not os.path.isfile(srcFilePath):
        print('Could not find input file: {}'.format(srcFilePath))
        return

    plotFilePath = None
    if args.EvaluationNr:
        dstDirPath = os.path.join(os.path.abspath(args.OutputBasePath), '{:0>4}'.format(args.EvaluationNr))
        dstFilePath = os.path.join(dstDirPath, srcFileName)
        plotFilePath = os.path.join(dstDirPath, options[0][0])
        if not os.path.isdir(dstDirPath):
            os.makedirs(dstDirPath)
        if not os.path.isfile(dstFilePath) or args.Force:
            shutil.copy2(srcFilePath, dstFilePath)
        else:
            print('Force disabled and file exists: {}'.format(dstFilePath))
    plotFunc = argsDict.get(options[1][0].replace('-', '_'))
    plotFunc(srcFilePath, plotFilePath)


def TrafficDiffPlotHandler(options, argsDict):
    #options[0][0]: sim traffic file path option key
    #options[1][0]: reference traffic file path option key
    #options[2][0]: plotting function option key
    simTrafficFileName = argsDict.get(options[0][0].replace('-', '_'))
    simTrafficFilePath = os.path.join(os.path.abspath(args.InputBasePath), simTrafficFileName)
    if not os.path.isfile(simTrafficFilePath):
        print('Could not find input file: {}'.format(simTrafficFilePath))
        return

    refTrafficFileName = argsDict.get(options[1][0].replace('-', '_'))
    refTrafficFilePath = os.path.join(os.path.abspath(args.InputBasePath), refTrafficFileName)
    if not os.path.isfile(refTrafficFilePath):
        print('Could not find input file: {}'.format(refTrafficFilePath))
        return

    plotFilePath = None
    if args.EvaluationNr:
        dstDirPath = os.path.join(os.path.abspath(args.OutputBasePath), '{:0>4}'.format(args.EvaluationNr))
        dstSimFilePath = os.path.join(dstDirPath, simTrafficFileName)
        dstRefFilePath = os.path.join(dstDirPath, refTrafficFileName)
        plotFilePath = os.path.join(dstDirPath, '{}-{}'.format(options[0][0], options[1][0]))
        if not os.path.isdir(dstDirPath):
            os.makedirs(dstDirPath)
        if not os.path.isfile(dstSimFilePath) or args.Force:
            shutil.copy2(simTrafficFilePath, dstSimFilePath)
        else:
            print('Force disabled and file exists: {}'.format(dstSimFilePath))
        if not os.path.isfile(dstRefFilePath) or args.Force:
            shutil.copy2(refTrafficFilePath, dstRefFilePath)
        else:
            print('Force disabled and file exists: {}'.format(dstRefFilePath))
    plotFunc = argsDict.get(options[2][0].replace('-', '_'))
    startingSecond = 3600*24*30 * max(0, argsDict['StartingMonth'] - 1)
    plotFunc(simTrafficFilePath, refTrafficFilePath, startingSecond, plotFilePath)


parser = argparse.ArgumentParser(description='Evaluates sim output')

parser.add_argument('EvaluationNr', type=int, nargs='?', help='Running number of evaluations')
parser.add_argument('--input-base-path', dest='InputBasePath', default='')
parser.add_argument('--output-base-path', dest='OutputBasePath', default='results')
parser.add_argument('--force', dest='Force', action='store_true')
parser.add_argument('--starting-month', type=int, dest='StartingMonth', default=1)

option_groups = [
    {'options': [('g2c-transfermgr-file', {'default': 'g2c_transfers.dat'}),
                 ('no-g2c-transfermgr-plot', {'action': 'store_const', 'const': NullPlot, 'default': PlotTransferMgr})],
    'handler': SimplePlotHandler
    },
    {'options': [('c2c-transfermgr-file', {'default': 'c2c_transfers.dat'}),
                 ('no-c2c-transfermgr-plot', {'action': 'store_const', 'const': NullPlot, 'default': PlotTransferMgr})],
    'handler': SimplePlotHandler
    },
    {'options': [('gcp-billing-file', {'default': 'GCP_storage.dat'}),
                 ('no-gcp-billing-plot', {'action': 'store_const', 'const': NullPlot, 'default': PlotBilling})],
    'handler': SimplePlotHandler
    },
    {'options': [('gcp-network-file', {'default': 'GCP_network.dat'}),
                 ('no-gcp-network-plot', {'action': 'store_const', 'const': NullPlot, 'default': NullPlot})],
    'handler': SimplePlotHandler
    },
    {'options': [('sim-traffic-file', {'default': 'sim_traffic.dat'}),
                 ('reference-traffic-file', {'default': 'reference_traffic.dat'}),
                 ('no-traffic-diff-plot', {'action': 'store_const', 'const': NullPlot, 'default': PlotTrafficDiff})],
    'handler': TrafficDiffPlotHandler
    },
]

for option_group in option_groups:
    for option in option_group['options']:
        parser.add_argument('--{}'.format(option[0]),**(option[1]))

args = parser.parse_args()
argsDict = vars(args)

for option_group in option_groups:
    option_group['handler'](option_group['options'], argsDict)

plt.show()
