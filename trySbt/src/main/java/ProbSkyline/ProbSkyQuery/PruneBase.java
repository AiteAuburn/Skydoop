package ProbSkyline.ProbSkyQuery;

import ProbSkyline.DataStructures.*;
import ProbSkyline.IO.*;
import mapreduce.ClusterConfig;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Properties;
import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.FileReader;
import java.io.BufferedReader;


public abstract class PruneBase{
	
	public ClusterConfig CC;
	public List<item> listItem;
	public ArrayList<PartitionInfo> outputLists;
	public HashMap<Integer, Boolean> ItemSkyBool;
	
	//----------------Key is objectID, value is its index in listItem-----------------
	public HashMap<Integer, Integer> corrIndex;

	public PruneBase(List<item> itemList, ClusterConfig CC){
		
		listItem = itemList;
		this.CC = CC;
		corrIndex = new HashMap<Integer, Integer>();
	}

	public void setItemSkyBool(){
	
		ItemSkyBool = new HashMap<Integer, Boolean>();
		if(listItem == null) 
			System.out.println("Sth Wrong in retrieving listItem.");

		for(int i=0; i<listItem.size();i++){
			int objectID = listItem.get(i).objectID;
			ItemSkyBool.put(objectID,true);
			corrIndex.put(objectID, i);
		}
	}


	@SuppressWarnings("unchecked")
	void readFile(){
		try{
			FileInputStream input = new FileInputStream(new File("MAX_MIN"));
			ObjectInputStream in = new ObjectInputStream(input);

			outputLists = ( ArrayList< PartitionInfo >)in.readObject();
			in.close();
			input.close(); 
		}
		catch(ClassNotFoundException cnfe){ cnfe.printStackTrace();  }
		catch(IOException e){ e.printStackTrace(); }
	}

	public abstract void preprocess();
	public abstract void prune();
}
