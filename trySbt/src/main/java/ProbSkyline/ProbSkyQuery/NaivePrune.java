package ProbSkyline.ProbSkyQuery;

import ProbSkyline.IO.TextInstanceWriter;
import ProbSkyline.IO.TextInstanceReader;

import ProbSkyline.DataStructures.*;
import ProbSkyline.IO.*;
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
		super(null,null);
	}


	@Override
	public void preprocess() {
//		super.init();
//		super.readFile();
//		super.setItemSkyBool();
	}

	public void itemsToinstances(){
		instances = new ArrayList<instance>();	
		
		for(int i=0; i<super.listItem.size(); i++){
			item aItem = super.listItem.get(i);
			for(int j=0; j<aItem.instances.size();j++){
				instances.add(aItem.instances.get(j));	
			}	
		}
	}

	public void prune() {
		compute();
	}

	public void compute(){
		//if(PruneMain.verbose)
			//log.info("in compute function instances size = "+ instances.size());

//		CompProbSky compProbSky = new NaiveHandler(super.listItem, super.dim, PruneBase.threshold, 2);
//		compProbSky.computeProb();
	}
}
