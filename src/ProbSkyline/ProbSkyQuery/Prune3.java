package ProbSkyline.ProbSkyQuery;

import ProbSkyline.DataStructures.*;
import ProbSkyline.IO.*;

import mapreduce.ClusterConfig;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Properties;
import java.util.Iterator;
import java.util.Map;


public class Prune3 extends PruneBase{
	private static org.apache.log4j.Logger log = Logger.getRootLogger();

	public List<instance> instances;
	public HashMap<Integer, Double> retMap;

	public Prune3(List<instance> aList, ClusterConfig CC){
		super(null, CC);
		instances = aList;	
	}

	public void setClusterConfig(ClusterConfig CC){
		super.CC = CC;	
	}

	@Override
	public void preprocess() {

	}

	public void setItemSkyBool( HashMap<Integer, Boolean> itemSkyBool ){
		
		super.ItemSkyBool = itemSkyBool;
	}

	public void setListItem( List<item> listItem  ){
		super.listItem = listItem;
	}

	public void itemsToinstances(){
		instances = new ArrayList<instance>();
		
		System.out.println("item size == "+ listItem.size());

		for(int i=0; i<listItem.size(); i++){
			item aItem = listItem.get(i);
			for(int j=0; j<aItem.instances.size();j++){
				instances.add(aItem.instances.get(j));
			}	
		}
	}

	@Override
	public void prune() {
		compute();
	}

	public void compObjSky(){
		retMap = new HashMap<Integer, Double>();
		for(int i=0; i<listItem.size(); i++){
			item curr = listItem.get(i);	
			double objSkyProb = 0.0;
			for(int j=0; j<curr.instances.size(); j++){
				instance aInst = curr.instances.get(j);
				objSkyProb += aInst.prob * aInst.instSkyProb;
			}
			if(objSkyProb >= CC.threshold){
				retMap.put(curr.objectID, objSkyProb);
				System.out.println("objectID = "+curr.objectID + "prob = "+objSkyProb);
			}
		}	
	}

	public void compute(){
		if( PruneMain.verbose )
			log.info("in compute function instances size = "+ instances.size());
		//CompProbSky compProbSky = new KDTreeHandler(instances, super.dim, super.ItemSkyBool);
		CompProbSky compProbSky = new WRTreeHandler(instances, CC.dim, super.ItemSkyBool, CC.numDiv - 1);
		compProbSky.computeProb();
		compObjSky();
	}
}
