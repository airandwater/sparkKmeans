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
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex,closest

#calculate coresets, go from 1 to many?? one point per machine (many)?
def calculateCoreset(p):
    sets = p
    #sets = union
    #sample t points from p
    wts = np.ones
    return((sets,wts))

def weighted_pick(probs):
    #return index 0:len(probs)-1 with weights given by probs
    probs=probs/probs.sum()
    cutoffs = probs.cumsum()
    #probs=probs/np.sum(probs)
    #cutoffs = np.cumsum(probs)
    idx = cutoffs.searchsorted(np.random.uniform(0, cutoffs[-1]))
    #print "idx is " + str(idx)
    return idx

def kmeansApprox(x,K,convergeDist):
    kPoints = x.takeSample(False, K, 1)
    tempDist = 1.0

    while tempDist > convergeDist:
        closest = x.map(lambda p: (closestPoint(p, kPoints)[0], (p, 1)))
        pointStats = closest.reduceByKey(lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))
        newPoints = pointStats.map(lambda (x, (y, z)): (x, y / z)).collect()

        tempDist = sum(np.sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)

        for (x, y) in newPoints:
            kPoints[x] = y
    return kPoints

def costSets(X,C):
    cst = 0
    for x in X: #make this some sort of reduce instead??
        #cst += np.min((x - C)**2)
        cst += closestPoint(x,C)[1]
    return(cst)

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

#def getCoresets(iterator,PBsum,tfactor):
def getCoresets(iterator):
    #iterator only iterates once, use it to collect list. is there a better way??
    Bc = [b for b in iterator] #when dont keep track of keys
    #B = Bc[0][0] #remove from the input altogether??
    m = Bc[0][1]
    Pind = Bc[0][2] #do this here or in a map before??
    Sind = []
    wS = [] #single
    #PBsum1 = PBsum.value
    #PBsum2 = PcostBC.value
    #print((PBsum1,PBsum2))
    print((len(Bc[0]),len(m),len(Pind)))
    #t = tfactor*np.sum(m)/PBsum
    t = tfactorBC.value*np.sum(m)/PcostBC.value
    print (t,np.ceil(t).astype(int))
    #t = tfactor*c/PBsum #tfactor*Bc[0][1]/PBsum
    #m = [closestPoint(p,Bc[0])[1] for p in pts]
    #select
    #for it in range(PcostBC.value):
    for it in range(np.ceil(t).astype(int)):
       q = weighted_pick(m) #with replacement, oh well shouldnt matter for large
       Sind.append(q) #change this to a set to begin with??
       #single
       print m[q]
       wS.append(PcostBC.value/(tfactorBC.value*m[q]))
    #vectorized
    #w = PcostBC.value/(tfactorBC.value*m[Sind])
    #SindSet = set(Sind) #set
    #the sum the indices Pind by coreset B, subtract weights of those from q 
    #loop through Sind, subtract w from the b that corresponds with Pind
    #for each coreset b, find the points whose closetsPoint(p,B)[0] match index of B
    #do this in a separate map function??
    #if P is passed in here, does all the memory get passed or only at evaluation?
    #Pind = [if closestPoint(p,B)[0]==m[p]]
    #calculate the number closest, subtract the number of points that appear in S
    #loop over points: add to Pb belonging to it and subtract w if index in Sind
    print wS
    wb = np.zeros(coresetK.value)
    for pnum, p in enumerate(Pind):
        wb[p] += 1
        #if pnum in SindSet: #set
            #wts[p] = wts[p] - w[pnum] #set
        if pnum in Sind:
            Sloc = Sind.index(pnum)
            print wb[p]
            wb[p] = wb[p] - wS[Sloc]
            print wb[p]
    yield (Sind,np.asarray(wS),wb) #combine outside this function in a final map with access to points?

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
    for it in len(PBsum)
       q = weighted_pick(m)
       Sind.append(q)
    w = PBsum/(tfactor*m[sind])
    #for each coreset b, find the points whose cost match closetsPoint(p,b)
    #if P is passed in here, does all the memory get passed or only at evaluation?
    Pind = [if closestPoint(p,b)==m[p]]
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
    print inds
    print S
    yield S
 
if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: kmeans <file> <k> <convergeDist> <n(optional)>"
        exit(-1)
    sc = SparkContext(appName="PythonKMeans")
    lines = sc.textFile(sys.argv[1])
    #data = lines.map(parseVector).cache()
    K = int(sys.argv[2])
    convergeDist = float(sys.argv[3])
    t=50 #argument?? 6? roughly kd = 6*4 for bad approximation
    tfactorBC = sc.broadcast(t)
    if len(sys.argv) == 5:
        n = int(sys.argv[4])
    else:
        n = 1

    #REDO EVERYTHING WITH PARTITIONS AS KEYS AND mapValues TO PRESERVE PARTITIONING?
    #DISTRIBUTE! make rdds and generate a coreset for each one
    #how do I determine data partitioning to compute separate B and t at each machine?
    #partition into n sections, change dynamically?
    #data._jrdd.splits().size()

    l2 = lines.repartition(n)
    data = l2.map(parseVector).cache()
    P2 = data.mapPartitionsWithIndex(addi,preservesPartitioning=True)
    P = data
    #P.glom().collect() #to check the partitioning
    #B = P.mapPartitions(lambda x: getRandB(x,K),preservesPartitioning=True).cache()
    #Pcost = B.map(lambda (k,v): v) #doesnt work now?
    #Pcost = B.flatMap(lambda (k,v): v)
    coresetK = sc.broadcast(K) #or something besides K for number of Bs
    #B points, distances (cost), and indices of closest B point
    B = P.mapPartitions(lambda x: getRandBwithinds(x),preservesPartitioning=True).cache()
    #Pcost = B.flatMap(lambda (k,v,v2): v)
    Pcostsum = B.flatMap(lambda (k,v,v2): v).reduce(add) #just pull out the costs to add
    PcostBC = sc.broadcast(Pcostsum)
    #ts = B.mapPartitions(getT,preservesPartitioning=True)
    #mapPartitionsWithIndex yield with append to tuple??
    #operate on P again, but add a tuple for the point in B which is closest to it??
    #S = B.mapPartitions(lambda x: getCoresets(x,Pcostsum,t),preservesPartitioning=True).collect()
    #now that mapPartitions, should mapPartitions be called again or just map?
    S = B.mapPartitions(lambda x: getCoresets(x),preservesPartitioning=True)#.cache()
    ##pass multiple RDD iterators into mapPartitions???
    #Piter = chain(Ppart.collect()) #???
    #S2 = B.mapPartitions(lambda x: getCoresetsWithPoints(x,Piter),preservesPartitioning=True).cache()
    #indices for S, weights for S, weights for B (could be in another function??)
    #final map (flatmap?): union of indices specified by S with B and union of wS and wB
    #C = S.flatMap(lambda (idx,ws,wb): [concatente(B,P[idx]), concatenate(wb,ws)]) #output many values
    #how do I merge with B first?? zip????
    #C = S.flatMap(lambda (idx,ws,wb): [(B,wb), (P[idx],ws)]) #output many values
    #Pwithind = P.zipWithIndex() #try this with updated SPARK! then group by key or something?
    ##ugly and slow but it works
    Sinds = S.map(lambda (idx,ws,wb): idx)
    #include points?? needed for PinS
    Ppart = P.mapPartitions(getPointsPartitioned,preservesPartitioning=True)
    Pwithinds = Sinds.zip(Ppart)
    PinS = Pwithinds.mapPartitions(getPointsInS,preservesPartitioning=True)
    #(PinS,ws) union (Bpts,wb)
    Swithweights = PinS.zip(S.map(lambda (idx,ws,wb): ws))
    Bpts = B.map(lambda (k,v,v2): k)
    Bwithweights = Bpts.zip(S.map(lambda (idx,ws,wb): wb))
    Coresets = Swithweights.union(Bwithweights)
    #its all there, now some formatting??
    CoresetsFinal = Coresets.reduce(concat) #do this earlier??
    CoresetPoints = np.vstack(np.asarray(CoresetsFinal)[0::2])
    CoresetWeights = np.hstack(np.asarray(CoresetsFinal)[1::2])
    CoresetsFinal2 = (CoresetPoints,CoresetWeights)
    #map 2 by 2?


    #another way to format output?
    # CPB = (S.zip(P)).zip(Bpts)
    # PB = P.zip(Bpts).cache()
    # CPB2 = S.zip(PB)
    # Cos = CPB.flatmap(lambda ((idx,ws,wb),(pt,b)): [(b,wb), (pt,ws)])
    # Cos = CPB2.mapPartitions(getFinalCoresetsWithWeights,preservesPartitioning=True)
    # #getitem(pt,idx)
    # Cosets = Cos.reduce(concat) #reduce with concatenate????
    #or cogroup and reduce by key??
    #or pass in iterator to partitions of P points, lookup source of mapPartitions
    #   def bl(iterator): yield [p for p in iterator]
    #   x=rdd.PipelinedRDD(P,lambda s,iterator: bl(iterator),preservesPartitioning=True)
    #or use a mapPartitionsWithIndex inside to get the pts with index i
    #C = B.mapPartitions(lambda x: getCoresetsBInput(x,B,Pcostsum,t),preservesPartitioning=True).cache()
    #Pcostsum = B.reduce(lambda (k,v): v + v)
    #Pcostsum = B.reduce(lambda a,b: a[1] + b[1])
    #Pcostsum = B.reduce(_ + _)
    #Pcostsum = B.reduce(add) #NO
    #
    #B = P.mapPartitions(lambda (k,v): k, preservesPartitioning=True)
    #P = dataKeys.zip(data.map(lambda x: x))
    #P = dataKeys.zip(data._reserialize())
    #P = data.map(lambda x: (x,1)).partitionBy(n)
    #Plines = lines.partitionBy(n).cache()
    #Pdata = P.mapPartitions(parseVector).cache()
    #solution to the entire dataset, return the cost
    #B=mapPartitionsWithIndex(lambda (splitIndex,iterator): (splitIndex, kmeansApprox(iterator,convergeDist/5)))
    #B=P.mapPartitions(lambda (iterator): kmeansApprox(iterator,K,convergeDist/5))
    #maybe glom all the ones in a partition?, then list instead of RDD
    #join??
    # PB=P.mapPartitions(
    #     lambda (iterator): (iterator,kmeansApprox(iterator,K,convergeDist/5)))
    # #calculate the cost of each one and sum, operate on 2 RDDs? P and B?
    # #cPB = P.zip(B).map()
    # cPB = PB.mapPartitionsWithIndex(lambda (iterator): costSets(iterator[0], iterator[1]))
    # #cPB = PB.mapPartitionsWithIndex(lambda (iter1,iter2): costSets(iter1, iter2))
    # print cPB
    #cPBsum = cPB.sum()
    #what is cost of 2 sets, cost of point and set (min cost point and any point in set)

    #calculate B_i for each vertex and then reduce to calculate the sum

    #initialize
    kPoints = data.takeSample(False, K, 1)
    tempDist = 1.0

    while tempDist > convergeDist:
        closest = data.map(
            lambda p: (closestPoint(p, kPoints), (p, 1)))
        pointStats = closest.reduceByKey(
            lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))
        newPoints = pointStats.map(
            lambda (x, (y, z)): (x, y / z)).collect()

        tempDist = sum(np.sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)

        for (x, y) in newPoints:
            kPoints[x] = y

    print "(Original) Final centers: " + str(kPoints)
