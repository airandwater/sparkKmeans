#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
The K-means algorithm written from scratch against PySpark. In practice,
one may prefer to use the KMeans algorithm in MLlib, as shown in
examples/src/main/python/mllib/kmeans.py.

This example requires NumPy (http://www.numpy.org/).
"""

import sys

import numpy as np
from pyspark import SparkContext
from pyspark import rdd
from operator import add,concat
from itertools import chain,imap
import time

def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])

def closestPoint(p, centers):
    #if centers is empty, return something constant for uniform sampling
    if not centers:
        return -1,30.
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = np.sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex,closest

def weightedClosestPoint(p, w, centers):
    #if centers is empty, return something constant for uniform sampling
    if not centers:
        return -1,30.
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = w*np.sum((p - centers[i]) ** 2)
        #print (len(centers),len(centers[i]),len(tempDist))
        #print "TEMPDIST IS" + str(tempDist)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex,closest

#REPLACE WITH sampleByKey ??!?!?!?!
def weighted_pick(probs):
    #return index 0:len(probs)-1 with weights given by probs
    probs=probs/probs.sum()
    cutoffs = probs.cumsum()
    #probs=probs/np.sum(probs)
    #cutoffs = np.cumsum(probs)
    idx = cutoffs.searchsorted(np.random.uniform(0, cutoffs[-1]))
    #print "idx is " + str(idx)
    return idx

def kmeansApprox(xw,K,convergeDist):
    kPoints = xw.map(lambda (k,w): k).takeSample(False, K, 1)
    tempDist = 1.0
    while tempDist > convergeDist:
        #closest = x.map(lambda p: (weightedClosestPoint(p, wt, kPoints)[0], (p, 1)))
        closest = xw.map(lambda (p,w): (weightedClosestPoint(p, w, kPoints)[0], (p, 1)))
        pointStats = closest.reduceByKey(lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))
        newPoints = pointStats.map(lambda (x, (y, z)): (x, y / z)).collect()
        tempDist = sum(np.sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)
        for (x, y) in newPoints:
            kPoints[x] = y
    return kPoints

def getRandBwithinds(iterator):
    #iterator only iterates once, use it to collect list. is there a better way??
    pts = [p for p in iterator] #when dont keep track of keys
    #print np.asarray(pts)
    B = []
    Ds = []    
    #if not pts: #dont bother filling, this should go away for large data
    if pts:
        #print "not empty points"
        for it in range(coresetK.value):
            #print it
            #calculate distances from each point to B
            #select one point weighted sampling based on distances
            #add point to B
            Ds = [closestPoint(p,B)[1] for p in pts]
            #print np.asarray(Ds)
            #print np.shape(np.asarray(Ds))
            c = weighted_pick(np.asarray(Ds))
            #B.append(p[c])
            B.append(pts[c])
            #print np.asarray(B)
        #here are all the points, do something with them: either random sampling or sample then swap??
    #else: print "empty points"
    #also yield Ds, to pass to all partitions??
    #final time
    #add ids = closestPoint(p,B)[0]
    CP = [closestPoint(p,B) for p in pts]
    Ds = np.asarray(CP)[:,1]
    ids = np.asarray(CP)[:,0]
    #print np.asarray(Ds).sum()
    yield ((B,Ds,ids))
    #yield ((B,np.asarray(CP))) #debug

def getPointsPartitioned(iterator):
    P = [p for p in iterator] #when dont keep track of keys
    yield P

def Sasmap(m):
    Sind = []
    wS = []
    t = tfactorBC.value*np.sum(m)/PcostBC.value
    for it in range(np.ceil(t).astype(int)):
       q = weighted_pick(m) #with replacement, oh well shouldnt matter for large
       Sind.append(q) #change this to a set to begin with??
       #single
       #print m[q]
       wS.append(PcostBC.value/(tfactorBC.value*m[q]))
    return(Sind, np.asarray(wS))

def getSweights(iterator):
    B = [b for b in iterator] #when dont keep track of keys
    m = B[0]
    #Pind = Bc[0][2] #do this here or in a map before??
    #Sind = []
    #wS = [] #single
    wFull = np.zeros_like(m)
    #PBsum1 = PBsum.value
    #PBsum2 = PcostBC.value
    #print((PBsum1,PBsum2))
    #t = tfactor*np.sum(m)/PBsum
    t = tfactorBC.value*np.sum(m)/PcostBC.value
    for it in range(np.ceil(t).astype(int)):
       q = weighted_pick(m) #with replacement, oh well shouldnt matter for large
       #Sind.append(q) #change this to a set to begin with??
       #single
       #wS.append(PcostBC.value/(tfactorBC.value*m[q]))
       wFull[q] = PcostBC.value/(tfactorBC.value*m[q])
    yield(wFull)


def getCoresetStreaming(iterator,PBsum,tfactor):
    #iterator only iterates once, use it to collect list. is there a better way??
    #B, c = [b[0], b[1] for b in iterator] #when dont keep track of keys
    #Bc = [(b[0][0], b[0][1]) for b in iterator] #when dont keep track of keys
    #Bc = [b[1] for b in iterator]
    for b in iterator:
        B = b[0]
        m = b[1]
    Sind = []
    print((len(Bc[0]),len(B),len(m)))
    PBsum1 = PBsum.value
    PBsum2 = PcostBC.value
    print((PBsum1,PBsum2))
    t = tfactor*np.sum(m)/PBsum
    #t = tfactor*c/PBsum #tfactor*Bc[0][1]/PBsum
    #m = [closestPoint(p,Bc[0])[1] for p in pts]
    #select
    for it in len(PBsum):
       q = weighted_pick(m)
       Sind.append(q)
    w = PBsum/(tfactor*m[sind])
    #for each coreset b, find the points whose cost match closetsPoint(p,b)
    #if P is passed in here, does all the memory get passed or only at evaluation?
    #Pind = [if closestPoint(p,b)==m[p]]
    #calculate the number closest, subtract the number of points that appear in S
    wts
    yield (Sind,wts)


def getCoresetsWithPoints(iterator,ptsIt):
    Bc = [b for b in iterator] #when dont keep track of keys
    #B = Bc[0][0] #remove from the input altogether??
    m = Bc[0][1]
    Pind = Bc[0][2] #do this here or in a map before??
    Sind = []
    wS = [] #single
    pts = ptsIt.next()
    print((len(Bc[0]),len(m),len(Pind)))
    print len(pts)
    yield len(m)

def getPointsInS(iterator):
    IP = [p for p in iterator]
    inds = IP[0][0]
    pts = IP[0][1]
    S = []
    for i in inds:
        S.append(pts[i])
    yield S

def getClusterError(tst,trth):
    erTot = 0
    # for p in np.arange(tst.shape[0]):
    #     cl = tst[p,:]
    for p in range(len(tst)):
        cl = tst[p]
        el = cl - trth
        cstl = np.amin(np.sum(el**2,axis=1),axis=0)
        erTot += cstl
    return erTot
 
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: kmeans <file> <k> <convergeDist> <n(optional)>"
        exit(-1)
    sc = SparkContext(appName="myPythonKMeans2")
    lines = sc.textFile(sys.argv[1])
    #data = lines.map(parseVector).cache()
    K = int(sys.argv[2])
    convergeDist = float(sys.argv[3])
    t=700 #argument?? 6? roughly kd = 6*4 for bad 700 for 2, 200b 800 for 3? 
    #also + nk*log(nk/delta)
    #6*4/.1^4 = for decent? maybe what matters is epsilon/d then k/(d^3 epsilon^4)
    tfactorBC = sc.broadcast(t)
    if len(sys.argv) == 5:
        n = int(sys.argv[4])
    else:
        n = 1

    #sampleByKey(withReplacement, fractions) to do nonuniform sampling directly on the RDDs ??
    #combine this with takeSample(withReplacement, num) somehow??


    #partition into n sections, change dynamically?
    # #create key value to partition by unique key, but then accessing by this unique pair requires collecting the RDD
    # #data._jrdd.splits().size()
    # #some other partitionBy function besides hash??
    # #P = data.repartition(n) #DOES NOT WORK!!! start from the source with lines
    
    l2 = lines.repartition(n)
    data = l2.map(parseVector).cache()
    #start timer?
    at1 = time.clock()
    P2 = data.zipWithUniqueId()
    #P3 = P2.zip(P2.map(lambda (v,i): i % n,preservesPartitioning=True))
    P4 = P2.map(lambda (v,i): (i % n,i/n,v),preservesPartitioning=True)
    #P.glom().collect() #to check the partitioning
    coresetK = sc.broadcast(K) #or something besides K for number of Bs
    #B points, distances (cost), and indices of closest B point
    B = data.mapPartitions(getRandBwithinds,preservesPartitioning=True).cache()
    #Pcost = B.flatMap(lambda (k,v,v2): v)
    Pcostsum = B.flatMap(lambda (k,v,v2): v).reduce(add) #just pull out the costs to add
    PcostBC = sc.broadcast(Pcostsum)
    #dont need v2 after all???
    Bpt = B.map(lambda (k,v,v2): k,preservesPartitioning=True).collect()
    Cpts = B.map(lambda (k,v,v2): v,preservesPartitioning=True)
    Clist = Cpts.collect()
    Blistind = B.map(lambda (k,v,v2): v2,preservesPartitioning=True).collect()
    P5 = P4.map(lambda (part,inx,p): (part,inx,p,Blistind[part][inx],Clist[part][inx]),preservesPartitioning=True) #maintain partition-dependent index
    #P5 = P4.map(lambda (part,inx,p): (part,inx,p,Bpt[part][int(Blistind[part][inx])],Clist[part][inx]),preservesPartitioning=True) #maintain actual point (cannot use as key)
    #maybe this is better way to calculate cost? maybe not since need to collect Clist...
    cPB = P5.map(lambda (part,inx,p,b,cpb): (part,cpb), preservesPartitioning=True).reduceByKey(add)
    Pcostsum2 = cPB.map(lambda (k,v): v).reduce(add) #also broadcast
    #wS = B.mapPartitions(getSweights,preservesPartitioning=True).cache()
    wS = Cpts.mapPartitions(getSweights,preservesPartitioning=True).cache()
    wSlist = wS.collect()
    #wSold = S.map(lambda (idx,ws,wb): ws, preservesPartitioning=True).collect() #any advantage to zip over mapping myself?
    Sptsweights = P4.filter(lambda (part,inx,p): wSlist[part][inx]>0).map(lambda (part,inx,p): (p,wSlist[part][inx]))
    #figure out how to assign inS and weightS to all points...
    #inS = S.map(lambda (idx,ws,wb): idx, preservesPartitioning=True).collect()
    #Pwithcenterandweights = P5.map(lambda (part,inx,p,b,cpb): (b,1-wSlist[part][inx])) #what about all the others?
    Pwithcenterandweights = P5.map(lambda (part,inx,p,b,cpb): ((part,b),1-wSlist[part][inx]),preservesPartitioning=True) #what about all the others?
    Bptsweights = Pwithcenterandweights.reduceByKey(add)
    Coreset = Sptsweights.union(Bptsweights.map(lambda ((part,b),wB): (Bpt[part][int(b)],wB)))
    #CoresetPoints = Coreset.map(lambda (p,w): p)
    #CoresetWeights = Coreset.map(lambda (p,w): w)
    
    #coreset approximation
    #kPointsApprox = kmeansApprox(Coreset,K,1)
    kPointsApprox = kmeansApprox(Coreset,K,convergeDist)
    #end timer, start new timer?
    at2 = time.clock()
    #original kmenas, initialize
    ot1 = time.clock()
    kPoints = data.takeSample(False, K, 1)
    tempDist = 1.0
    while tempDist > convergeDist:
        closest = data.map(
            lambda p: (closestPoint(p, kPoints)[0], (p, 1)))
        pointStats = closest.reduceByKey(
            lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))
        newPoints = pointStats.map(
            lambda (x, (y, z)): (x, y / z)).collect()
        tempDist = sum(np.sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)
        for (x, y) in newPoints:
            kPoints[x] = y
    #end timer
    ot2 = time.clock()
    #better way to get length besides another action?
    numD = data.count()
    numC = Coreset.count()
    #print output
    print "(Approximate) Final centers: " + str(kPointsApprox)
    print "(Original) Final centers: " + str(kPoints)
    #verify?? first match the centers to each other
    if sys.argv[1] == 'kmeans_data2.txt':
        truth = np.array(([0, 0, 0, 0, 0, 0],[3, 0, 0, 4, 0, 7],[1, 3.5, 0, 0, 9.2, 0],[0, 7.2, 0, 6, 0, 0]))
    elif sys.argv[1] == 'kmeans_data3.txt':
        truth = np.loadtxt('kmeans_centers3.txt')
    elif sys.argv[1] == 'kmeans_data4.txt':
        truth = np.loadtxt('kmeans_centers4.txt')
    elif sys.argv[1] == 'kmeans_data5.txt': #too big?
        truth = np.loadtxt('kmeans_centers5.txt')
    #is this a good metric, or compare output to true clustering?
    Eapprox = getClusterError(kPointsApprox,truth)
    Eexact = getClusterError(kPoints,truth)
    print str(numD) + ' original points, ' + str(numC) + ' coreset points'
    #print str(len(Clist)) + ' original points, ~' + str(len(Bpt+t) + ' coreset points'
    print "Approximate MSE: " + str(Eapprox) + ", Original MSE: " + str(Eexact)
    print "Approximate error per point: " + str(Eapprox/numD) + ", Original error per point: " + str(Eexact/numD)
    print "Approximate time: " + str(at2 - at1) + ", Original time: " + str(ot2-ot1) + ", Speedup: " + str((ot2-ot1)/(at2-at1)) + "x"
    sc.stop()


