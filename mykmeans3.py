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

def getRandB(iterator):
    #iterator only iterates once, use it to collect list. is there a better way??
    pts = [p for p in iterator] #when dont keep track of keys
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
    #CP = [closestPoint(p,B) for p in pts]]
    #Ds = CP[1]
    #ids = CP[0]
    #yield ((B,np.asarray(Ds),np.asarray(ids)))
    Ds = [closestPoint(p,B)[1] for p in pts]
    #print np.asarray(Ds)
    #print np.asarray(Ds).sum()
    #yield ((B,np.asarray(Ds).sum())) #always ok???
    yield ((B,np.asarray(Ds))) #now all the sums just added, get the individual at the end
    #yield ((B,np.asarray(Ds),np.asarray(Ds).sum())) #store all the costs here or recalculate?

def getRandBwithindspoints(iterator):
    #iterator only iterates once, use it to collect list. is there a better way??
    pts = [p for p in iterator] #when dont keep track of keys
    #print np.asarray(pts)
    B = []
    Ds = []    
    #if not pts: #dont bother filling, this should not occur for any reasonable input
        #print "not empty points"
    if pts:
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
    for it in range(len(pts)):
        #print (it,partid)
        #yield (B[it],Ds[it],ids[it])
        yield (pts[it],ids[it].astype(int),Ds[it])
    #yield ((B,np.asarray(CP))) #debug

def getSpointsweights(partid,iterator): #(p,b,cpb)
    p = []
    bind = []
    m = []
    for b in iterator:
        p.append(b[0])
        bind.append(b[1])
        m.append(b[2])
    # for pp,b,cpb in iterator: #any difference?
    #     p.append(pp)
    #     bind.append(b)
    #     m.append(cpb)

    m=np.asarray(m)
    #t = tfactor*np.sum(m)/PBsum
    t = tfactorBC.value*np.sum(m)/PcostBC.value
    for it in range(np.ceil(t).astype(int)):
        q = weighted_pick(m) #with replacement, oh well shouldnt matter for large
        #Sind.append(q) #change this to a set to begin with??
        #print "index chosen is " + str(q)
        #wFull[q] = PcostBC.value/(tfactorBC.value*m[q])
        #yield one at a time
        yield(bind[q], p[q], PcostBC.value/(tfactorBC.value*m[q]))
    #OR on all points: B get weight 0, S get 1 - whatsabove, others get 1
    #include all points (p,b,cpb,w)  can I get the rest (B weights) from this??
    #filter by those less than 1 and cost nonzero, give (value - 1)*-1
    #OR new mapPartitions, all we need is the b so subtract weight from that one
    #COMBINE WITH THE FUNCTION getBweight???

def getSpointsweightsStreaming(partid,iterator):
    #select sampP*Pthiscost[0] points with weighted probabilities c/Pthiscost[0]
    #in expectation this should be sampP points...
    #selected any of these times 1-(1-c/Pthiscost[0])^sampP*Pthiscost[0]
    #assume something (Pcosts) is broadcast with ('partid':(Pcostsum,num))
    #itertools.compress() instead?
    Pthiscost = Pcosts.value[str(partid)]
    #sampP = tfactorBC.value/PcostBC.value #only broadcast this one?
    sampP = TPratio.value
    for p,bind,c in iterator:
        #if np.random.uniform(0,1) < sampP*Pthiscost[0]/Pthiscost[1]: #WRONG
        if np.random.uniform(0,1) < (1 - (1 - c/Pthiscost)**(sampP*Pthiscost)):
            yield (bind,p,1/(sampP*c))

def getBweight(partid,iterator):
    p = []
    bind = []
    c = []
    for b in iterator:
        p.append(b[0])
        bind.append(b[1])
        c.append(b[2])
    bind = np.asarray(bind)
    p = np.asarray(p)
    c = np.asarray(c)
    for b in np.unique(bind):
        #B = p[bind==b and c==0]
        B = p[np.logical_and(bind==b, c==0)]
        #print B #hopefully only 1, if not take the min?
        w = sum(bind==b)
        #print w #could be any
        #yield((partid,b),B,np.sum(w)-1) #remove one for the point itself
        yield("%d-%d"%(partid,b),(B,w-1)) #remove one for the point B itself
    # for b in np.unique(bind):
    #     yield("%d,%d"%(partid,b),B,np.sum(w)-1) #remove one for the point B itself

def getBweightCorrection(partid,iterator):
    bind = []
    wt = []
    for b in iterator:
        bind.append(b[0])
        wt.append(b[2])
    wt = np.asarray(wt)
    for b in np.unique(bind):
        w = wt[bind==b]
        #yield((partid,b),0,-np.sum(w)) #add 0 in the place of B for reduce
        yield("%d-%d"%(partid,b),(0,-np.sum(w))) #add 0 in the place of B for reduce
    
def partitionsum(iterator): 
    yield sum(iterator)

def partitionsumID(partid,iterator): 
    yield(str(partid), sum(iterator))

def partitionsumIDcount(partid,iterator): 
    c = 0
    s = 0
    for it in iterator:
        c+=1
        s+=it
    yield(str(partid), (s,c))

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
    sc = SparkContext(appName="myPythonKMeans3")
    K = int(sys.argv[2])
    convergeDist = float(sys.argv[3])
    t=700 #argument?? 6? roughly kd/eps^4 = 6*4 for bad, also + nk*log(nk/delta) but negligible if large d or number of points?
    #6*4/.1^4 = for decent? maybe what matters is epsilon/d then k/(d^3 epsilon^4)
    if len(sys.argv) == 5:
        #n = int(sys.argv[4])
        lines = sc.textFile(sys.argv[1], minPartitions=int(sys.argv[4]))
    else:
        lines = sc.textFile(sys.argv[1])


    #keep track of everything with only (partitionid,point,costToB)
    #sample nonuniformly somehow
    #sample this=t*reduceByKey(cost)/reduce(cost) from each partition
    #sample each one wp this/points per partition
    #if costToB nonzero, weight = 1/this w.p. this and 0 w.p. (1-this)
    #filter with a random variable < p???
    #if costToB zero, recalculate costs to these points?

    #DISTRIBUTE! make rdds and generate a coreset for each one
    #how do I determine data partitioning to compute separate B and t at each machine?
    #partition into n sections, change dynamically?
    
    data = lines.map(parseVector).cache()
    at1 = time.clock()
    #data.getNumPartitions()
    #data.glom().collect() #to check the partitioning
    coresetK = sc.broadcast(K) #or something besides K for number of Bs
    
    B = data.mapPartitions(getRandBwithindspoints,preservesPartitioning=True).cache()
    
    # # #all in all out
    # tfactorBC = sc.broadcast(t)
    # Pcostsum = B.map(lambda (p,b,cpb): cpb, preservesPartitioning=True).mapPartitions(partitionsum,preservesPartitioning=True).sum() #if partition not output above
    # #cPB_2 = P5_2.map(lambda (part,p,b,cpb): (part,cpb), preservesPartitioning=True).reduceByKey(add)
    # #Pcostsum3 = cPB_2.map(lambda (k,v): v, preservesPartitioning=True).sum() #also broadcast
    # PcostBC = sc.broadcast(Pcostsum)
    # SptsBweights1 = B.mapPartitionsWithIndex(getSpointsweights,preservesPartitioning=True).cache()
    
    # #same in expectation, but streaming?
    # sampProb = tfactorBC*cPB_2/PcostBC
    # wS = P5_2.map(lambda (part,p,b,cpb): (p,0 if cpb==0 else (np.rand()<sampProb/num)*1/sampProb),preservesPartitioning=True)
    # Sptsweights_2 = wS.map(lambda (): (p,wS)).filter(lambda (): w > 0)
    Pcostvec = B.map(lambda (p,b,cpb): cpb, preservesPartitioning=True).mapPartitionsWithIndex(partitionsumID,preservesPartitioning=True)
    #Pcostsum3 = Pcostvec.map(lambda (k,(v,n)): v, preservesPartitioning=True).sum() #also broadcast
    Pcostsum3 = Pcostvec.values().sum() #also broadcast
    TPratio = sc.broadcast(t/Pcostsum3)
    Pcdict = Pcostvec.collectAsMap()
    Pcosts = sc.broadcast(Pcdict)
    SptsBweights1 = B.mapPartitionsWithIndex(getSpointsweightsStreaming,preservesPartitioning=True).cache()


    Bptsweights1 = B.mapPartitionsWithIndex(getBweight,preservesPartitioning=True).cache()
    #within partitions reduce by key, but problem: centers B are not equally weighted
    #recalculate? for all with cost = 0, reduce by the B index corresponding to that
    toCorrect = SptsBweights1.mapPartitionsWithIndex(getBweightCorrection,preservesPartitioning=True).cache()
    #Bptsweights = Bptsweights1.union(toCorrect).reduceByKey(lambda (B1,w1), (B2,w2): (B1+B2,w1+w2)).map(lambda (s,(B,w)): (B,w)).cache() #will this reduce both values?
    Bptsweights = Bptsweights1.union(toCorrect).reduceByKey(lambda (B1,w1), (B2,w2): (B1+B2,w1+w2)).values().cache() #will this reduce both values?
    SptsBweights = SptsBweights1.map(lambda (bind,S,w): (S,w))
    Coreset = SptsBweights.union(Bptsweights)
    #print "CORESET IS LENGTH " + str(Coreset.count())
    #print Coreset.take(5)

    #coreset approximation
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
    #sc.stop()


