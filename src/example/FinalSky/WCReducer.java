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
				int objectID = curr.objectID;
				if(aMap.containsKey(objectID) == false){

					item aItem = new item(objectID);		  
					aMap.put(objectID, aItem);
				}
				item currItem = aMap.get(objectID);
				currItem.addInstance(curr);
				if(line[1].equalTo("R"))
					rList.add(curr);
			}

			List<item> itemList = new ArrayList<item>(aMap.values());
			if(itemList.size() <1)
				System.out.println("Sth Wrong in retrieveing itemList");
			else{
				System.out.println("in WCReduce ---itemList size = "+ itemList.size());
				System.out.println("in WCReduce ---instList size = "+ rList.size());
			}

   /*         HashMap<Integer, Double>  retMap = client.prune(itemList);*/
			//if(retMap != null){
				//Iterator it = retMap.entrySet().iterator();
				//while (it.hasNext()) {
					//Map.Entry pairs = (Map.Entry)it.next();
					//System.out.println(pairs.getKey() + " = " + pairs.getValue());
					//out.collect( String.valueOf(pairs.getKey()), String.valueOf(pairs.getValue()) );
				//}		
			//}
			//else
				/*System.out.println("Sth wrong in looking for retMap.");*/
		}
}
