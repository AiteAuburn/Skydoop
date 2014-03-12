package SingleClient

import scala.collection.mutable.ListBuffer
import ProbSkyline.PartitionMain
import mapreduce.ClusterConfig
import java.io.File
import scala.collection.mutable.HashSet
import scala.io.Source

object DrawCurve{

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