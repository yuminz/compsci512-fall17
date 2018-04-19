package slicer

import akka.actor.{ActorRef}

import scala.collection.mutable

class Rebalancer() {

  private var spreadedSlices: Set[PartitionInfo] = Set()

  class MappingBuilder {

    class KeyRange(val start: Long, val size: Long) {
      override def equals(obj: scala.Any): Boolean = {
        obj match {
          case other: KeyRange => {
            if (this.eq(other)) {
              true
            } else {
              this.start == other.start && this.size == other.size
            }
          }
          case _ => false
        }
      }

      override def hashCode(): Int = {
        start.hashCode() + size.hashCode()
      }
    }

    val keysToServers = new mutable.HashMap[KeyRange, Set[ActorRef]]

    def add(slice: PartitionInfo): Unit = {
      // see if set currently contains slice, if so merge the two and readd
      val range = new KeyRange(slice.start, slice.size)
      val current = keysToServers.get(range)
      if (current.isDefined) {
        // merge two sets
        keysToServers.put(range, current.get ++ slice.servers)
      } else {
        // put new entry
        keysToServers.put(range, slice.servers.toSet)
      }
    }

    def get(slice: PartitionInfo): Option[Set[ActorRef]] = {
      val range = new KeyRange(slice.start, slice.size)
      keysToServers.get(range)
    }

    def getMapping(): mutable.MutableList[PartitionInfo] = {
      // convert set to list and return
      val mapping = new mutable.MutableList[PartitionInfo]
      for (k <- keysToServers.iterator) {
        val p = new PartitionInfo(k._1.start, k._1.size, k._2.toList)
        mapping += p
      }
      mapping
    }
  }

  def reduceSpread(utilizations: mutable.Map[ActorRef, LoadStats], lowerBound: Long, upperBound: Long): Unit = {
    // we can modify utilizations in place
    // each time make new LoadStats
    //println("reduce spread called with " + spreadedSlices.size + " slices")
    for (slice <- spreadedSlices) {
      // get all load Stats for current slice, build into a set
      val currentLoadStats = new mutable.HashSet[(ActorRef, LoadStats)]()
      slice.servers.foreach(c => currentLoadStats.add((c, utilizations(c))))
      //println("current key is " + slice.start)
      //println("current load stats is: " + currentLoadStats.toString())

      var canStillMerge = true
      while (canStillMerge) {
        if (currentLoadStats.size > 1) {
          //println("current load stats size is " + currentLoadStats.size)
          // get highest and lowest LoadStats, see if we can put highest in lowest cache, if so remove both from
          // current load stats, replace highest's LoadStats with updated, add updated lowest's to currentLoadStats
          val max = currentLoadStats.maxBy(e => e._2.heat)
          val min = currentLoadStats.minBy(e => e._2.heat)

          //println(s"max found is $max")
          //println(s"min found is $min")

          val maxHeat = max._2.partitionMap(slice)
          //println(s"max heat is $maxHeat , with lower bound $lowerBound and upperBound $upperBound")
          if (max != min && max._2.heat - maxHeat > lowerBound && min._2.heat + maxHeat < upperBound) {
            //println("we can merge the two")
            currentLoadStats.remove(max)
            currentLoadStats.remove(min)

            val newMaxHeat = max._2.heat - maxHeat
            val newMaxLoadStats = new LoadStats(newMaxHeat / 1000d, newMaxHeat, max._2.partitionMap - slice)
            utilizations.put(max._1, newMaxLoadStats)

            val newMinHeat = min._2.heat + maxHeat
            val newKeyHeat = maxHeat + min._2.partitionMap(slice)
            val newMinLoadStats = new LoadStats(newMinHeat / 1000d, newMinHeat, min._2.partitionMap + Tuple2[PartitionInfo, Long](slice, newKeyHeat))
            currentLoadStats.add((min._1, newMinLoadStats))
          } else {
            //println("we cannot merge the two")
            canStillMerge = false
          }
          // if can't merge, set canStillMerge to false
        } else {
          canStillMerge = false
        }
      }

      val remainingCaches = currentLoadStats.map(_._1)
      for (remaining <- currentLoadStats) {
        //utilizations.put(remaining._1, remaining._2)
        val oldLoadStats = remaining._2

        val newMap = (oldLoadStats.partitionMap - slice) + Tuple2[PartitionInfo, Long](new PartitionInfo(slice.start, slice.size, remainingCaches.toList), oldLoadStats.partitionMap.get(slice).get)
        utilizations.put(remaining._1, new LoadStats(oldLoadStats.utilization, oldLoadStats.heat, newMap))
      }
    }
  }

  def rebalanceAssignment(utilizations: mutable.Map[ActorRef, LoadStats]): (mutable.MutableList[PartitionInfo], Long) = {

    //val mapping = new mutable.MutableList[PartitionInfo]
    // any time we get returned slices that should be added
    //val mapping = new mutable.HashMap[KeyRange, Set[ActorRef]]
    //val mapping = new mutable.HashSet[PartitionInfo]()
    val mappingBuilder = new MappingBuilder

    //1. Calculate total heat and divide by numCaches => average heat per cache
    var totalHeat : Long = 0
    for (u <- utilizations.keySet){
      //println("heat for " + u.path.name + " is " + utilizations.get(u).get.heat)
      totalHeat += utilizations.get(u).get.heat;
    }
    val averageHeat = totalHeat / utilizations.size

    val bound = (averageHeat * 0.1).asInstanceOf[Long]
    val upperBound = averageHeat + bound
    val lowerBound = averageHeat - bound
    reduceSpread(utilizations, lowerBound, upperBound)
    //println("state of utilization after merge: " + utilizations.toString())

    //2. move the slices from overheated cache to surplus pool
    var surplusPool = Map[PartitionInfo, Long]()
    //println(s"total heat is $totalHeat")
    //println(s"average heat is $averageHeat")
    for (u <- utilizations.keySet){
      if (utilizations.get(u).get.heat > averageHeat){
        //println("currently looking at" + u.path.name)
        //println("partition map is: " + utilizations.get(u).get.toString)
        var surplusAmount = utilizations.get(u).get.heat - averageHeat

        val (keep, addToSurplus) = pickBestSlices(utilizations.get(u).get.partitionMap, surplusAmount)
        surplusPool ++= addToSurplus
        //mapping ++= keep.keySet
        for (k <- keep.keySet) {
          mappingBuilder.add(k)
        }
      }
    }

    // compute number of keys being remapped
    //println(surplusPool)
    val keysMoved = surplusPool.keySet.foldLeft(0l)((b, p) => b + p.size)

    for (u <- utilizations.keySet){
      if (utilizations.get(u).get.heat <= averageHeat) {
        // add all the old slices in deficit cache to mapping
        val temporary = new mutable.MutableList[PartitionInfo]()
        temporary ++= utilizations.get(u).get.partitionMap.keySet

        val deficitAmount = (averageHeat - utilizations.get(u).get.heat).asInstanceOf[Long]
        if (deficitAmount > 0){
          val (remainInSurplus, addToCurrentCache) = pickBestSlices(surplusPool, deficitAmount)
          surplusPool = remainInSurplus
          temporary ++= addToCurrentCache.keySet.map((p: PartitionInfo) => new PartitionInfo(p.start, p.size, List(u)))
        }
        val sortedSlices = temporary.sortBy(p => p.start)
        for (i <- sortedSlices.indices) {
          val current = sortedSlices.get(i).get
          if (i + 1 < sortedSlices.length) {
            val next = sortedSlices.get(i + 1).get
            // currently we just don't merge if either is a singleton, wait to do later when we know for sure its spread is == 1
            if (next.start == current.start + current.size && canMergeSlice(current, mappingBuilder, surplusPool) && canMergeSlice(next, mappingBuilder, surplusPool) ) {
              // merge
              sortedSlices.update(i + 1, new PartitionInfo(current.start, current.size + next.size, current.servers))
            } else {
              //mapping += current
              mappingBuilder.add(current)
            }
          } else {
            //mapping += current
            mappingBuilder.add(current)
          }
        }
      }
    }
    if (surplusPool.size > 0) {
      for (s <- surplusPool) {
        val p = s._1
        //mapping += new PartitionInfo(p.start, p.size, utilizations.seq.head._1)
        mappingBuilder.add(new PartitionInfo(p.start, p.size, List(utilizations.seq.head._1)))
      }
    }
    val mapping = mappingBuilder.getMapping()
    spreadedSlices = mapping.toSet.filter(p => p.servers.length > 1)
    (mapping, keysMoved)
  }

  def canMergeSlice(slice: PartitionInfo, mappingBuilder: MappingBuilder, surplusPool: Map[PartitionInfo, Long]): Boolean = {
    if (slice.servers.size == 1) {
      val set = mappingBuilder.get(slice)
      if (set.isEmpty || set.get.size <= 1) {
        return !surplusPool.contains(slice)
      }
    }
    false
  }

  def pickBestSlices(heatInfo: Map[PartitionInfo, Long], targetHeat: Long): Tuple2[Map[PartitionInfo, Long], Map[PartitionInfo, Long]] = {
    if (heatInfo.nonEmpty) {
      val bound = (targetHeat * 0.1).asInstanceOf[Long]
      val upperBound = targetHeat + bound
      val lowerBound = targetHeat - bound

      val sortedSlices = heatInfo.toVector.sortBy(_._2)
      // first check if smallest is greater than upper bound, if so, go directly to split procedure
      //println("sorted slices: " + sortedSlices)
      //println("upper bound: " + upperBound)
      if (sortedSlices.head._2 < upperBound) {
        //println("sorted slices is: " + sortedSlices)
        //println(s"lower bound: $lowerBound and upperBound $upperBound")
        val idx = binarySearch(sortedSlices, lowerBound, upperBound).get
        val entry = sortedSlices(idx)
        if (entry._2 >= lowerBound && entry._2 < upperBound) {
          // go ahead and put entry into the pool and we are done
          // return here
          val addToPool = Map(entry)
          val keep = sortedSlices.toMap - entry._1
          return (keep, addToPool)
        } else {
          // starting at entry, have double for loop going through all entries less than or equal to upperBound
          val largestToSmallest = sortedSlices.reverse
          val reversedIndex = sortedSlices.length - 1 - idx
          // these indices will be for the largestToSmallest!!
          val indices = new mutable.HashSet[Int]()
          for (i <- reversedIndex until largestToSmallest.length) {
            val entryI = largestToSmallest(i)
            var runningSum = entryI._2
            indices.clear()
            indices.add(i)
            for (j <- i + 1 until largestToSmallest.length) {
              val entryJ = largestToSmallest(j)
              if (runningSum + entryJ._2 < upperBound) {
                runningSum += entryJ._2
                indices.add(j)
                if (runningSum >= lowerBound) {
                  return convertIndicesToTuple(indices, largestToSmallest)
                }
              }
            }
          }
          // if we find way to take away heat, return here
        }
      }
      splitProcedure(sortedSlices.reverse, lowerBound, upperBound, targetHeat)
    } else {
      // return empty
      (Map[PartitionInfo, Long](), Map[PartitionInfo, Long]())
    }
  }

  private def splitProcedure(largestToSmallest: Vector[(PartitionInfo, Long)], lowerBound: Long, upperBound: Long, targetHeat: Long): Tuple2[Map[PartitionInfo, Long], Map[PartitionInfo, Long]] = {
    var runningSum = 0l
    //println("target heat of " + targetHeat)
    //println("largest to smallest is: " + largestToSmallest.toString())
    val toSurplusPool = new mutable.HashMap[PartitionInfo, Long]()
    val keep = new mutable.HashMap[PartitionInfo, Long]()
    // initialize keep to be all of largest to smallest, remove from it every time we add to toSurplusPool
    keep ++= largestToSmallest
    for (c <- largestToSmallest) {
      if (c._2 + runningSum < upperBound) {
        runningSum += c._2
        toSurplusPool.put(c._1, c._2)
        keep.remove(c._1)
        if (runningSum >= lowerBound) {
          // return stuff
          //println("this shouldn't happen")
          //val keep = largestToSmallest.toMap -- toSurplusPool.keySet
          return (keep.toMap, toSurplusPool.toMap)
        }
      } else if (c._1.size == 1) {
        // increase spread of key, add one half to keep and one to surplus pool
        // since we are building new slices, we can't just compute keep as largestToSmallest - toSurplusPool
        val oldPartition = c._1
        if (c._2 / 2 + runningSum <= upperBound) {
          runningSum += c._2 / 2
          keep.remove(oldPartition)

          val keepSlice = new PartitionInfo(oldPartition.start, 1, oldPartition.servers)
          val surplusSlice = new PartitionInfo(oldPartition.start, 1, oldPartition.servers)
          val newHeat = c._2 / 2
          keep.put(keepSlice, newHeat)
          toSurplusPool.put(surplusSlice, c._2 - newHeat)

          //println("increasing spread of key " + oldPartition.start)

          if (runningSum >= lowerBound) {
            //println("exiting through increase in spread")
            return (keep.toMap, toSurplusPool.toMap)
          }
        } else {
          // in this case, even increasing the spread and halving the heat would be too hot, so exclude this one
          //
          //println("cant increase spread")
        }
      } else {
        val percentToKeep = (c._2 + runningSum - targetHeat)/c._2.asInstanceOf[Double]
        val oldPartition = c._1
        var keepSize = (oldPartition.size * percentToKeep).asInstanceOf[Long]
        if (keepSize == 0) {
          keepSize = 1
        }

        val startForSurplusSlice = oldPartition.start + keepSize
        val heatForKeepSlice = c._2 + runningSum - targetHeat
        val keepSlice: PartitionInfo = new PartitionInfo(oldPartition.start, keepSize, oldPartition.servers)
        //val keep = ((largestToSmallest.toMap - oldPartition) + Tuple2(keepSlice, heatForKeepSlice)) -- toSurplusPool.keySet
        keep.remove(oldPartition)
        keep.put(keepSlice, heatForKeepSlice)

        val surplusSize = oldPartition.size - keepSize
        // check if surplusSize is greater than zero, if not, don't create a new surplus slice
        if (surplusSize > 0) {
          val surplusSlice: PartitionInfo = new PartitionInfo(startForSurplusSlice, surplusSize, oldPartition.servers)
          val heatForSurplusSlice = c._2 - (c._2 + runningSum - targetHeat)
          toSurplusPool.put(surplusSlice, heatForSurplusSlice)
        }

        return (keep.toMap, toSurplusPool.toMap)
      }
    }
    //println("insufficient slices to fill deficit, returning best attempt")
    //(Map[PartitionInfo, Long](), largestToSmallest.toMap)
    (keep.toMap, largestToSmallest.toMap)
  }

  private def convertIndicesToTuple(indices: mutable.HashSet[Int], vec: Vector[(PartitionInfo, Long)]): (Map[PartitionInfo, Long], Map[PartitionInfo, Long]) = {
    val toSurplusPool = new mutable.HashMap[PartitionInfo, Long]()
    indices.foreach(i => toSurplusPool.put(vec(i)._1, vec(i)._2))
    val keep = vec.toMap -- toSurplusPool.keySet
    (keep, toSurplusPool.toMap)
  }

  // this method returns one of the slices in the range or the highest heat slice less than lower
  def binarySearch(l: Vector[(PartitionInfo, Long)], lower: Long, upper: Long): Option[Int] = {
    var low = 0
    var hi = l.length - 1
    while (low <= hi) {
      val mid = (low + hi) / 2
      val midInfo = l(mid)._2
      if (upper == lower && lower == midInfo) {
        return Some(mid)
      }
      if (upper <= midInfo) {
        hi = mid - 1
      } else if (lower <= midInfo) {
        // heat is in range
        return Some(mid)
      } else {
        low = mid + 1
      }
    }
    Some(hi)
  }


  def computeAverageUtilizations(currentCacheLoads: mutable.Map[ActorRef, mutable.MutableList[LoadStats]]): (mutable.Map[ActorRef, LoadStats], Double, ActorRef, ActorRef) = {
    val avgUtils = new mutable.HashMap[ActorRef, LoadStats]()
    var maxLoad = -1d
    var maxCache: ActorRef = null
    var minLoad = 2d
    var minCache: ActorRef = null
    var totalUtils: Double = 0
    for (cache <- currentCacheLoads.keySet) {
      //println("computing average util for cache " + cache.path.name)
      val loads = currentCacheLoads.get(cache).get
      var aggregateLoadStats: LoadStats = null
      for (l <- loads) {
        //println("total heat " + l.heat)
        //println("partition heat " + l.partitionMap.seq.head._2)
        if (aggregateLoadStats == null) {
          aggregateLoadStats = l
        } else {
          aggregateLoadStats = aggregateLoadStats + l
        }
      }
      val avgUtilization = aggregateLoadStats.utilization / loads.length
      val avgHeat = aggregateLoadStats.heat / loads.length
      val avgHeatMap = new mutable.HashMap[PartitionInfo, Long]()
      for (s <- aggregateLoadStats.partitionMap.keySet) {
        val agg = aggregateLoadStats.partitionMap.get(s).get
        avgHeatMap.put(s, agg / loads.length)
      }
      val avgLoadStats = new LoadStats(avgUtilization, avgHeat, avgHeatMap.toMap)
      avgUtils.put(cache, avgLoadStats)
      totalUtils += avgLoadStats.utilization
      if (avgLoadStats.utilization > maxLoad) {
        maxLoad = avgLoadStats.utilization
        maxCache = cache
      }
      if (avgLoadStats.utilization < minLoad) {
        minLoad = avgLoadStats.utilization
        minCache = cache
      }
    }
    val totalAverage = totalUtils / currentCacheLoads.keySet.size
    Tuple4(avgUtils, totalAverage, maxCache, minCache)
  }
}
