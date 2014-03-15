package SingleClient

import scala.collection.mutable.ListBuffer
import java.io.File
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.io.Source

import mapreduce.ClusterConfig;
import ProbSkyline.PartitionMain;
import ProbSkyline.DataStructures._;



object Util{

	val CC = ClusterConfig.getInstance()

	/*
	 * get the whole map data[Int, item], filter objects whose item is still in 
	 * objectSet, which is returned by the first phase.
	 */
	def getItemList(objSet: HashSet[Int]) ={

		var aMap = new HashMap[Integer, item]();
		val pm = new PartitionMain()
		for(line <- Source.fromFile(CC.srcName + ".txt").getLines()){

			val curr = pm.stringToInstance(line);
			if(objSet.contains(curr.objectID)){
				if(!aMap.contains(curr.objectID)){
					val aItem = new item(curr.objectID);              
					aMap.update(curr.objectID, aItem);
				}
				aMap(curr.objectID).addInstance(curr);
			}
		}
		val itemList = aMap.values.toList
		itemList
	}
}
