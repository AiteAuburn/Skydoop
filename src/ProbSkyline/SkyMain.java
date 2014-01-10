package ProbSkyline;
import mapreduce.ClusterConfig;
import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;

import ProbSkyline.ProbSkyQuery.Prune1And2;
import ProbSkyline.ProbSkyQuery.Prune3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

public class SkyMain{

	ClusterConfig CC;
	List<item> itemList;

	public SkyMain(ClusterConfig CC){
		this.CC= CC;	
	}

	public instance stringToInstance(String instString){

		String [] div = instString.split(" ");
		instance inst = null;
		if(div.length == CC.dim+3){

			inst= new instance(Util.getInstID(div[1]), Util.getObjectID(div[0]), Util.getProb(div[div.length-1]), CC.dim);
			inst.setPoint(Util.getPoint(div, CC.dim));
		}
		else
			System.out.println("Sth Wrong in creating instance.");

		return inst;
	}


	public void getItemList(String inputFile){

		SkyClient client = new SkyClient(ClusterConfig.getInstance()); 
		HashMap<Integer, item> aMap = new HashMap<Integer, item>();

		try{
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = br.readLine();
		while( line != null){

			instance curr = stringToInstance(line);
			int objectID = curr.objectID;
			if(aMap.containsKey((Integer)objectID) == false){

				item aItem = new item(objectID);              
				aMap.put(objectID, aItem);
			}
			item currItem = aMap.get(objectID);
			currItem.addInstance(curr);
			line = br.readLine();
		}
		}
		catch(IOException ioe){
			ioe.printStackTrace();	
		}
		this.itemList = new ArrayList<item>(aMap.values());
	}



	public void prune(List<item> itemList){
		Prune1And2 P12 = new Prune1And2(itemList, CC);	
		P12.prune();
		List<instance> afterPrune12List = P12.itemsToinstances();

		Prune3 P3 = new Prune3(afterPrune12List, CC);
		P3.setListItem(P12.itemsToItems());
		P3.setClusterConfig(CC);
		P3.setItemSkyBool(P12.ItemSkyBool);
		P3.prune();
	}


	public static void main(String [] args){

		SkyMain main = new SkyMain(ClusterConfig.getInstance());        
		main.getItemList("4.txt");
		if(main.itemList != null)
			main.prune(main.itemList);
		else
			System.out.println("Sth Wrong in retrieving itemList in getItemList");
	}

}
