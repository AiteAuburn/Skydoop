package SingleClient

import scala.collection.mutable.ListBuffer
import ProbSkyline.PartitionMain
import mapreduce.ClusterConfig
import java.io.File

object SingleClient extends App{
	
	/**
	 * some variables initialization of preprocess
	 */
	val CC = ClusterConfig.getInstance()
	var fList = ListBuffer[File]()
	val partPath: String = CC.partFolder
	listAllFiles(fList, new File(partPath))
	var main = new PartitionMain();


	/**
	 * file List iteration.
	 */
	var iter = 0
	for(f <- fList){
		ClusterConfig.replace("testArea", iter)
		println(f.getAbsolutePath())
		main.getItemList(f.getAbsolutePath());
		main.prune(main.itemList)
		iter += 1
	}


	/**
	 * get the files uner a folder recursively.
	 * Since part folder also has other files, the function filters files which are ".txt" files
	 */
	def listAllFiles(fList: ListBuffer[File], path:File){
		if(path.isDirectory()){
			for{f<-path.listFiles() if f.getName().substring(f.getName().length()-3) == "txt" }{
				if(f.isFile) fList += f.getAbsoluteFile() 
				else listAllFiles(fList, f)
			}
		}
	}
}
