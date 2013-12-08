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

public class NaivePrune extends PruneBase{
	private static org.apache.log4j.Logger log = Logger.getRootLogger();

	public List<instance> instances;

	public NaivePrune(){
		super();
		this.preprocess();	
		//itemsToinstances();
	}

	public NaivePrune(List<item> aList){
		
		super.listItem= aList;	
	}

	@Override
	protected void preprocess() {
		super.init();
		super.readFile();
		super.setItemSkyBool();
	}

	public void itemsToinstances(){
		instances = new ArrayList<instance>();	
		
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

	public void compute(){
		//if(PruneMain.verbose  )
			//log.info("in compute function instances size = "+ instances.size());

		CompProbSky compProbSky = new NaiveHandler(super.listItem, super.dim, PruneBase.threshold, 2);
		compProbSky.computeProb();
	}
}
