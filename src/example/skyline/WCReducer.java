package example.skyline;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;
import mapreduce.ClusterConfig;
import ProbSkyline.SkyClient;
import mapreduce.*;

public class WCReducer extends Reducer {
	@Override
		public void reduce(String key, Iterator<String> values, Outputer out) {
			long sum = 0;
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

			List<item> itemList = new ArrayList<item>(aMap.values());
			if(itemList.size() <1)
				System.out.println("Sth Wrong in retrieveing itemList");
			else
				System.out.println("in WCReduce ---itemList size = "+ itemList.size());

			HashMap<Integer, Double>  retMap = client.prune(itemList);
			if(retMap != null){
				Iterator it = retMap.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pairs = (Map.Entry)it.next();
					System.out.println(pairs.getKey() + " = " + pairs.getValue());
					out.collect( String.valueOf(pairs.getKey()), String.valueOf(pairs.getValue()) );
				}		
			}
			else
				System.out.println("Sth wrong in looking for retMap.");
		}
}
