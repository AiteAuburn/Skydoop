package ProbSkyline.ProbSkyQuery;

import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;


import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public class NaiveHandler implements CompProbSky {

	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	/*
	 * input to this class is a list of instance.
	 */
	public List<item> itemList;

	public int dim;
	public double threshold;
	public int area;
	public HashSet<Integer> candidates;
	public HashSet<Integer> all;

	public NaiveHandler (List<item> aList, int dim, double threshold, int area){
		this.dim = dim;	
		this.itemList= aList;
		this.threshold = threshold;
		this.area = area;
	}

	@SuppressWarnings("unchecked")
	void computeInfo(){

	}

	//@Override
	public void computeProb( ){
		
		candidates = new HashSet<Integer>();
		all = new HashSet<Integer>();

		for(int i=0; i<itemList.size(); i++){
			item aItem = itemList.get(i);	
			
			int itemID = aItem.objectID;

			for(int j=0; j<aItem.instances.size(); j++){
				instance aInst = aItem.instances.get(j);	
				int instID = aInst.instanceID;
				
				double skyProb = 1.0;
				for(int k=0; k<itemList.size(); k++){

					item itemOther = itemList.get(k);
					if(itemID == itemOther.objectID) continue;

					double itemAddition = 0.0;
					for(int l=0; l<itemOther.instances.size(); l++){
						instance instOther = itemOther.instances.get(l);	

						if(instOther.DominateAnother(aInst))
							itemAddition += instOther.prob;
					}
					//if(instID == 2805 && itemAddition>0){
						//log.info("2805's objectID = "+ itemID);
						//log.info("debug  itemAddition objectID = "+ itemOther.objectID + " itemAddition= " + itemAddition);
					//}

					skyProb *= (1.0 - itemAddition);
				}
				aInst.instSkyProb = skyProb;
				//if(skyProb >0 )
				//log.info("a inst ID = "+ instID + "  skyProb = " + skyProb);
				//System.out.println("curr ID = "+ aInst.instanceID + " instSkyProb = "+ skyProb);
			}
		}

		System.out.println("Probability threshod = " + this.threshold);
		int numObject = 0;
		for(int i=0; i<itemList.size(); i++){
			item aItem = itemList.get(i);	
			
			int itemID = aItem.objectID;
			double objSkyProb = 0.0;
			for(int j=0; j<aItem.instances.size(); j++){
				instance aInst = aItem.instances.get(j);	

				objSkyProb += aInst.prob * aInst.instSkyProb;
			}
			if(objSkyProb > 0.0){
				all.add(itemID);	
			}
			if(objSkyProb > this.threshold){
				log.info("an object ID = "+ itemID + "  skyProb = " + objSkyProb);
				numObject++;
				candidates.add(itemID);
			}
		}
		
		File candiFile = new File("./output/candi_"+ Integer.toString(area) );
		File allFile = new File("./output/all_"+ Integer.toString(area) );

		try{

			FileOutputStream fileOut = new FileOutputStream(candiFile);
			ObjectOutputStream outStream = new ObjectOutputStream(fileOut);
			outStream.writeObject(candidates);
			outStream.flush();
			outStream.close();
			fileOut.close();

		}catch(IOException e){ e.printStackTrace();  }

		try{

			FileOutputStream fileOut = new FileOutputStream(allFile);
			ObjectOutputStream outStream = new ObjectOutputStream(fileOut);
			outStream.writeObject(all);
			outStream.flush();
			outStream.close();
			fileOut.close();

		}catch(IOException e){ e.printStackTrace();  }

		System.out.println("After Prune 3 the item size = " + numObject);
	}
}
