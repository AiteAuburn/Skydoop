package ProbSkyline.ProbSkyQuery;

import mapreduce.ClusterConfig;
import ProbSkyline.ProbSkyQuery.CompProbSky;

import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;

import java.util.List;
import java.util.ArrayList;


@SuppressWarnings("rawtypes")
public class InstNaive implements CompProbSky {

	/*
	 * input to this class is a list of instance.
	 */
	public List<item> lItemList;
	public List<instance> rInstList;

	public ClusterConfig CC;

	public InstNaive(List<item> lItemList, List<instance> rInstList){
		this.lItemList= lItemList;
		this.rInstList= rInstList;
	}

	@SuppressWarnings("unchecked")
	void computeInfo(){

	}

	/**
	 * based on every instance, it compute every instance's skyline probability.
	 */
	public void computeProb( ){

		for(instance aInst: rInstList){
			int instID = aInst.instanceID;

			double skyProb = 1.0;
			for(int k=0; k<lItemList.size(); k++){

				item itemOther = lItemList.get(k);
				if(aInst.objectID== itemOther.objectID) continue;

				double itemAddition = 0.0;
				for(int l=0; l<itemOther.instances.size(); l++){
					instance instOther = itemOther.instances.get(l);	
					if(instOther.DominateAnother(aInst))
						itemAddition += instOther.prob;
				}
				skyProb *= (1.0 - itemAddition);
			}
			aInst.instSkyProb = skyProb;
		}
	}
}
