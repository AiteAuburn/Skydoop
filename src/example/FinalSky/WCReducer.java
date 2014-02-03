package example.FinalSky;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;
import mapreduce.ClusterConfig;
import ProbSkyline.InterClient;
import mapreduce.*;

public class WCReducer extends Reducer {
	@Override
		public void reduce(String key, Iterator<String> values, Outputer out) {
			long sum = 0;
			HashMap<Integer,item > aMap = new HashMap<Integer, item>(); 
			ArrayList<instance> rList = new ArrayList<instance>();
			InterClient client = new InterClient(ClusterConfig.getInstance()); 

			while(values.hasNext()){
				String interString = values.next();

				/*
				 * convert String to Instance in InterClient.java
				 */
				String [] line = interString.split(" ");
				instance curr = client.stringToInstance(line);
				if(line[1].equals("L")){
					int objectID = curr.objectID;
					if(aMap.containsKey(objectID) == false){

						item aItem = new item(objectID);		  
						aMap.put(objectID, aItem);
					}
					item currItem = aMap.get(objectID);
					currItem.addInstance(curr);
				}
				else
					rList.add(curr);
			}

			List<item> itemList = new ArrayList<item>(aMap.values());
			client.comp(itemList, rList);
			for(instance inst: rList){
				out.collect(inst.toString(), "");
			}
		}
}
