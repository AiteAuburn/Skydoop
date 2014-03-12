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

	/**
	 * get the object hashset, which contains all unique object.
	 */
	def getObjSet() ={
		var fList = ListBuffer[File]()
		val partPath: String = CC.partFolder
		listAllFiles(fList, new File(partPath))

		var objSet = new HashSet[Int]
		for(f <- fList){
			for(line <- Source.fromFile(f).getLines())
				objSet += line.toInt
		}
		println(objSet.size)
		objSet
	}

	/*
	 * get the whole map data[Int, item], filter objects whose item is still in 
	 * objectSet, which is returned by the first phase.
	 */
	def getItemList(objSet: HashSet[Int]) ={

		var aMap = new HashMap[Integer, item]();
		for(line <- Source.fromFile(CC.srcName + ".txt").getLines()){

			val curr = PartitionMain.stringToInstance(line);
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

	/**
	 * get the files uner a folder recursively.
	 * Since part folder also has other files, the function filters files which are "*lt" files
	 */
	def listAllFiles(fList: ListBuffer[File], path:File){
		if(path.isDirectory()){
			for{f<-path.listFiles() if f.getName().substring(f.getName().length()-2) == "lt" }{
				if(f.isFile) fList += f.getAbsoluteFile()
				else listAllFiles(fList, f)
			}
		}
	}



	def main(args: Array[String]) {
		val objSet = getObjSet();
		
	}

}