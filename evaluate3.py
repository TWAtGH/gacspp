#!/usr/bin/env python

import argparse
import json
import os
import shutil
import statistics
import time

import psycopg2
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description='Evaluates sim output')


def GetQueryFromFile(path):
    try:
        with open(path, 'r') as f:
            return f.read()
    except Exception as err:
        print('Failed to get query from file: {}'.format(err))
    return ''

def NewPlot(name, plotFunc, *args):
    resW = 1600#1920
    resH = 720#1080
    dpi = 160
    plt.figure(num=name, figsize=(resW/dpi, resH/dpi), dpi=dpi)
    plt.ticklabel_format(axis='both', style='plain')
    plt.grid(axis='y')
    plotFunc(name, *args)

def PlotFileSize(name, dbCursor):
    print('FileSize plot...')
    q = GetQueryFromFile(os.path.join('queries', 'filesize.sql'))

    t1 = time.time()
    print('\tquery...')
    dbCursor.execute(q)
    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\tfetching...')
    row = dbCursor.fetchone()
    bins = [row[1], row[2]]
    weights = [row[3]]
    for row in dbCursor.fetchall():
        bins.append(row[2])
        weights.append(row[3])

    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\tplotting...')
    plt.hist(bins[:-1], bins=bins, weights=weights)
    print('\ttook: {}s'.format(time.time() - t1))

    plt.xlabel('Filesize/MiB')
    plt.ylabel('Count')
    plt.legend()
    plt.axis(xmin=0, ymin=0)
    plt.title('Filesize distribution')
    plt.savefig('{}.png'.format(name))

def PlotNumReplicasAtStorageElement(name, dbCursor, storageElementName):
    print('PlotNumReplicasAtStorageElement plot...')
    q = GetQueryFromFile(os.path.join('queries', 'numReplicasAtStorageElement.sql'))
    q = q.replace('<storageelementname>', storageElementName)

    t1 = time.time()
    print('\tquery...')
    dbCursor.execute(q)
    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\tfetching...')
    rows = []
    x = []
    for row in dbCursor.fetchall():
        rows.append(row)
        x.append(row[0])
        if( x[-1] != row[1] ):
            x.append(row[1])

    print('\ttook: {}s'.format(time.time() - t1))


    t1 = time.time()
    print('\ttransforming...')
    y = [0] * len(x)
    x.sort()
    lastStartIdx = 0
    for row in rows:
        while x[lastStartIdx] < row[0]:
            lastStartIdx += 1
        for i in range(lastStartIdx, len(x)):
            if x[i] > row[1]:
                break
            y[i] += row[2]

    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\tplotting...')
    plt.plot(x, y)
    print('\ttook: {}s'.format(time.time() - t1))

    plt.xlabel('Time/s')
    plt.ylabel('Number of replicas')
    plt.legend()
    plt.axis(xmin=0, ymin=0)
    plt.title('Num replicas at {}'.format(storageElementName))
    plt.savefig('{}.png'.format(name))

def PlotTransferTimeStats(name, dbCursor):
    print('PlotTransferTimeStats plot...')
    q = GetQueryFromFile(os.path.join('queries', 'transferTimeStats.sql'))

    t1 = time.time()
    print('\tquery...')
    dbCursor.execute(q)
    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\tfetching...')
    x = []
    y = ([], [])
    for row in dbCursor.fetchall():
        x.append(row[0])
        y[0].append(row[1])
        y[1].append(row[2])

    print('\ttook: {}s'.format(time.time() - t1))


    t1 = time.time()
    print('\ttransforming...')

    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\tplotting...')
    plt.plot(x, y[0], color='red', label='Avg. queue time')
    #plt.plot(x, y[1], color='tan', label='Avg. finish time')
    print('\ttook: {}s'.format(time.time() - t1))

    plt.xlabel('Time/s')
    plt.ylabel('Time/s')
    plt.legend()
    plt.axis(xmin=0, ymin=0)
    plt.title('Average transfer times')
    plt.savefig('{}.png'.format(name))

def PlotTransferNumStats(name, dbCursor, interval=(3600*6)):
    print('PlotTransferNumStats plot...')
    q = GetQueryFromFile(os.path.join('queries', 'transferNumQueuedStats.sql'))

    t1 = time.time()
    print('\tquery1...')
    dbCursor.execute(q)
    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\tfetching1...')
    x = []
    y = []
    for row in dbCursor.fetchall():
        x.append(row[0])
        y.append(row[1])
    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\ttransforming1...')
    bins = [0]
    weights1 = []
    i = 0
    for t in range(0, x[-1], interval):
        bins.append(t)
        weights1.append(0)
        while x[i] < t:
            weights1[-1] += y[i]
            i += 1
    print('\ttook: {}s'.format(time.time() - t1))


    q = GetQueryFromFile(os.path.join('queries', 'transferNumStartedStats.sql'))

    t1 = time.time()
    print('\tquery2...')
    dbCursor.execute(q)
    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\tfetching2...')
    x = []
    y = []
    for row in dbCursor.fetchall():
        x.append(row[0])
        y.append(row[1])
    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\ttransforming2...')
    weights2 = [0] * (len(bins) - 1)
    i = 0
    j = 0
    for t in range(0, x[-1], interval):
        if t > bins[-1]:
            bins.append(t)
            weights1.append(0)
        weights2[j] = 0
        while x[i] < t:
            weights2[j] += y[i]
            i += 1
        j += 1
    print('\ttook: {}s'.format(time.time() - t1))

    t1 = time.time()
    print('\tplotting2...')
    plt.hist([bins[:-1], bins[:-1]], bins, weights=[weights1, weights2],  stacked=True, color=['red', 'tan'], label=['Num queued', 'Num started'])
    print('\ttook: {}s'.format(time.time() - t1))

    plt.xlabel('Time/{}h bins'.format(int(interval/3600)))
    plt.ylabel('Count')
    plt.legend()
    plt.axis(xmin=0, ymin=0)
    plt.title('Number of queued and started transfers')
    plt.savefig('{}.png'.format(name))

tstart = time.time()
connectionStr = ''
try:
    with open(os.path.join('config', 'psql_connection.json'), 'r') as f:
        connectionStr = json.load(f).get('connectionStr')
except Exception as err:
    print('Failed to get connection str from config: {}'.format(err))


conn = psycopg2.connect(connectionStr)
dbCursor = conn.cursor()


NewPlot('FileSize', PlotFileSize, dbCursor)
NewPlot('TransferTimes', PlotTransferTimeStats, dbCursor)
NewPlot('TransferCounts', PlotTransferNumStats, dbCursor, 3600*6)
#NewPlot('NumReplicasBNL_DATADISK', PlotNumReplicasAtStorageElement, dbCursor, 'BNL_DATADISK')
#NewPlot('NumReplicasIowa_bucket', PlotNumReplicasAtStorageElement, dbCursor, 'iowa_bucket')

print('{}s'.format(time.time() - tstart))

plt.show()

conn.close()
