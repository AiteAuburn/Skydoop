package example.skyline;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;
import mapreduce.ClusterConfig;
import ProbSkyline.SkyClient;
import mapreduce.*;

public class WCReducer extends Reducer {

  @Override
  public void reduce(String key, Iterator<String> values, Outputer out) {

    long sum = 0;
	List<item> itemList = new ArrayList<item>();
	HashMap<Integer,item > aMap = new HashMap<Integer, item>(); 

	SkyClient client = new SkyClient(ClusterConfig.getInstance()); 

    while(values.hasNext()){
	  String currInstString = values.next();

	  /*
	   * convert String to Instance in SkyClient.java
	   */
	  instance curr = client.stringToInstance(currInstString);
	  int objectID = curr.objectID;
	  if(aMap.containsKey(objectID) == false){
	
	    item aItem = new item(objectID);		  
		aMap.put(objectID, aItem);
	  }
	  item currItem = aMap.get(objectID);
	  currItem.addInstance(curr);
    }
    
    //out.collect(key, Long.toString(sum));
  }
}
