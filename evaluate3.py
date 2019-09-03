#!/usr/bin/env python

import argparse
import os
import shutil
import statistics
import time

import psycopg2
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description='Evaluates sim output')


tstart = time.time()

conn = psycopg2.connect('dbname=gacspp user=postgres password=123')
db = conn.cursor()


q = "SELECT t.starttick, r.storageelementname, count(t.id), sum(r.filesize/(1024*1024)) "\
"FROM transfers t "\
"LEFT JOIN (SELECT r.id AS id, s.name AS storageelementname, f.filesize AS filesize "\
		   "FROM replicas r, files f, storageelements s "\
		   "WHERE r.fileid = f.id AND r.storageelementid = s.id) r "\
"ON t.srcreplicaid = r.id "\
"GROUP BY r.storageelementname, t.starttick "\
"ORDER BY r.storageelementname, t.starttick"

q='select width_bucket(starttick, 0, 2592000, 1000) as buckets, count(*) from transfers t, replicas r where t.dstreplicaid = r.id AND r.storageelementid = 20 group by buckets order by buckets;'
#q='select starttick from transfers t, replicas r where t.dstreplicaid = r.id AND r.storageelementid = 20;'
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

x = []
y = []
for t in db.fetchall():
	x.append(t[0])
	y.append(t[1])
#x=[t[0] for t in db.fetchall()]
print('took: {}s'.format(time.time() - t1))

t1 = time.time()
print('transforming...')
plt.plot(x, y)
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
