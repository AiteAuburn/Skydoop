package org.liang.ProbSkyQuery;

import org.liang.IO.TextInstanceWriter;
import org.liang.IO.TextInstanceReader;

import org.liang.DataStructures.*;
import org.liang.IO.*;

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
	
	public int dim;
	public int maxObjectNum;
	public int ObjectNum;
	public String testArea;
	public static double threshold;

	public List<item> listItem;
	public ArrayList<PartitionInfo> outputLists;
	public HashMap<Integer, Boolean> ItemSkyBool;
	
	//----------------Key is objectID, value is its index in listItem-----------------
	public HashMap<Integer, Integer> corrIndex;

	public static double[] split_Value;
	public static int MaxSplitNum = 10;
	public static int SplitNo;

	void init(){

		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("liang.prop"));;

			split_Value = new double[MaxSplitNum];
			FileReader fd = new FileReader("liang.split");

			BufferedReader br = new BufferedReader(fd);
			int split_index = 0;
			String line = br.readLine();
			while(line != null && split_index < MaxSplitNum){

				if(Double.parseDouble(line) >=0 ){
			
				split_Value[split_index] = Double.parseDouble(line);
				split_index ++;
				}

			line = br.readLine();
			}
			SplitNo = split_index;

	    	}catch (IOException ex) {
	    		ex.printStackTrace();
		}

		this.maxObjectNum = Integer.parseInt(prop.getProperty("objectNum"));
		this.dim = Integer.parseInt(prop.getProperty("dim"));
		this.testArea = prop.getProperty("testArea");
		threshold = Double.parseDouble(prop.getProperty("threshold"));
		
		//System.out.println("PruneBase threshold = "+threshold);
		corrIndex = new HashMap<Integer, Integer>();
		listItem = new ArrayList<item>(this.maxObjectNum);
	}

	public void setItemSkyBool(){
	
		ItemSkyBool = new HashMap<Integer,Boolean>();
		for(int i=0; i<listItem.size();i++){
			int objectID = listItem.get(i).objectID;
			ItemSkyBool.put(objectID,true);
			corrIndex.put(objectID, i);
		}
	}

	public PruneBase(){
	//	init();
	//	setItemSkyBool();
	}

	@SuppressWarnings("unchecked")
	void readFile(){
		try{
			TextInstanceReader TIR = new TextInstanceReader(testArea +  ".txt");
			TIR.setDim(dim);
			TIR.readListItem(listItem);
			TIR.close();
			ObjectNum = listItem.size();
			FileInputStream input = new FileInputStream(new File("MAX_MIN"));
			ObjectInputStream in = new ObjectInputStream(input);

//			if(in.readObject() instanceof ArrayList){
				
				outputLists = ( ArrayList< PartitionInfo >)in.readObject();
//			}
			in.close();
			input.close(); 
		}
		catch(ClassNotFoundException cnfe){ cnfe.printStackTrace();  }
		catch(IOException e){ e.printStackTrace(); }
	}

	@SuppressWarnings("unchecked")
	void readFile( int testAreaInt){
		try{
			TextInstanceReader TIR = new TextInstanceReader(Integer.toString(testAreaInt)+  ".txt");
			TIR.setDim(dim);
			TIR.readListItem(listItem);
			TIR.close();
			ObjectNum = listItem.size();
			FileInputStream input = new FileInputStream(new File("MAX_MIN"));
			ObjectInputStream in = new ObjectInputStream(input);

//			if(in.readObject() instanceof ArrayList){
				
				outputLists = ( ArrayList< PartitionInfo >)in.readObject();
//			}
			in.close();
			input.close(); 
		}
		catch(ClassNotFoundException cnfe){ cnfe.printStackTrace();  }
		catch(IOException e){ e.printStackTrace(); }
	}

	protected abstract void preprocess();
	protected abstract void prune();

}
