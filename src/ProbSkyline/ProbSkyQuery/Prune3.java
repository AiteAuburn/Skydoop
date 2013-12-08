package org.liang.ProbSkyQuery;

import org.liang.IO.TextInstanceWriter;
import org.liang.IO.TextInstanceReader;

import org.liang.DataStructures.*;
import org.liang.IO.*;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Properties;
import java.util.Iterator;
import java.util.Map;

import org.liang.ClusterConfig;

public class Prune3 extends PruneBase{
	private static org.apache.log4j.Logger log = Logger.getRootLogger();

	public List<instance> instances;
	public ClusterConfig CC;

	public Prune3(){
		super();
		this.preprocess();	
		itemsToinstances();
	}

	public Prune3(List<instance> aList){
		
		instances = aList;	
	}

	public void setClusterConfig(ClusterConfig CC){
		this.CC = CC;	
	}

	@Override
	protected void preprocess() {
		super.init();
		super.readFile();
		super.setItemSkyBool();
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
	protected void prune() {
		compute();
	}

	public void compObjSky(){
		for(int i=0; i<listItem.size(); i++){
			item curr = listItem.get(i);	
			double objSkyProb = 0.0;
			for(int j=0; j<curr.instances.size(); j++){
				instance aInst = curr.instances.get(j);
				objSkyProb += aInst.prob * aInst.instSkyProb;
			}
		if(objSkyProb >= CC.threshold)
			log.info("objSkyProb = "+  objSkyProb);
		}	
	}

	public void compute(){
		if( PruneMain.verbose )
			log.info("in compute function instances size = "+ instances.size());
		//CompProbSky compProbSky = new KDTreeHandler(instances, super.dim, super.ItemSkyBool);
		CompProbSky compProbSky = new WRTreeHandler(instances, super.dim, super.ItemSkyBool,CC.numDiv - 1);
		compProbSky.computeProb();
		compObjSky();
	}
}
