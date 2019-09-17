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


tstart = time.time()
connectionStr = ''
try:
    with open(os.path.join('config', 'psql_connection.json'), 'r') as f:
        connectionStr = json.load(f).get('connectionStr')
except Exception as err:
    print('Failed to get connection str from config: {}'.format(err))


conn = psycopg2.connect(connectionStr)
db = conn.cursor()


q = GetQueryFromFile(os.path.join('queries', 'filesize.sql'))

resW = 1920
resH = 1080
dpi = 100
plt.figure(num=1, figsize=(resW/dpi, resH/dpi), dpi=dpi)
plt.ticklabel_format(axis='both', style='plain')
plt.grid(axis='y')

t1 = time.time()
print('query...')
db.execute(q)
print('took: {}s'.format(time.time() - t1))

t1 = time.time()
print('fetching...')
#t = db.fetchone()
#print(t)
#sid = t[1]
#x = [t[0]]
#y = [t[3]]
#for t in db.fetchall():
#	if t[1] != sid:
#		plt.plot(x, y, label=sid)
#		sid = t[1]
#		x = [t[0]]
#		y = [t[3]]
#	else:
#		x.append(t[0])
#		y.append(y[-1] + t[3])

x = db.fetchall()
y = 200

#x=[t[0] for t in db.fetchall()]
print('took: {}s'.format(time.time() - t1))

t1 = time.time()
print('transforming...')
plt.hist(x, y)
print('took: {}s'.format(time.time() - t1))


plt.xlabel('Sim Time')
plt.ylabel('Count')
plt.legend()
plt.axis(xmin=0, ymin=0)

t1 = time.time()
print('saving...')
plt.savefig('test.png')
print('took: {}s'.format(time.time() - t1))

print('plotting...')
print('{}s'.format(time.time() - tstart))

plt.show()

conn.close()
