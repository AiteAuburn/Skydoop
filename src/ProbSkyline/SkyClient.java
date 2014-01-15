package ProbSkyline;
import mapreduce.ClusterConfig;
import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;

import ProbSkyline.ProbSkyQuery.Prune1And2;
import ProbSkyline.ProbSkyQuery.Prune3;

import java.util.List;
import java.util.HashMap;

public class SkyClient{

	ClusterConfig CC;
	instance aInst;

	public SkyClient(ClusterConfig CC, String line){
		this.CC= CC;	
		String [] div = line.split(" ");
		if(div.length == CC.dim+3){
			aInst = new instance( Util.getInstID(div[1]), Util.getObjectID(div[0]), Util.getProb(div[div.length-1]), CC.dim);
			aInst.setPoint(Util.getPoint(div, CC.dim));
		}
		else
			System.out.println("Sth Wrong in creating instance.");
	}

	public SkyClient(ClusterConfig CC){
		this.CC= CC;	
	}

	public instance stringToInstance(String instString){

		String [] div = instString.split(" ");
		instance inst = null;
		if(div.length == CC.dim+3){
			inst= new instance( Util.getInstID(div[1]), Util.getObjectID(div[0]), Util.getProb(div[div.length-1]), CC.dim);
			inst.setPoint(Util.getPoint(div, CC.dim));
		}
		else
			System.out.println("Sth Wrong in creating instance.");

		return inst;
	}

	/**
	 * Based on the instance's position, its partition number is computed.
	 */	
	public int getPartition(){
		if(aInst != null)
			return aInst.partition(CC.splitValue, CC.numWorkers);	

		return -1;
	}

	/**
	 * Do Prunning work in an partition.
	 */
	public HashMap<Integer, Double> prune(List<item> itemList){
		System.out.println("after reading mapper intermidiate result--- itemList = "+itemList.size());
		Prune1And2 P12 = new Prune1And2(itemList, CC);	
		P12.prune();
		List<instance> afterPrune12List = P12.itemsToinstances();

		Prune3 P3 = new Prune3(afterPrune12List, CC);
		P3.setListItem(P12.itemsToItems());
		P3.setClusterConfig(CC);
		P3.setItemSkyBool(P12.ItemSkyBool);
		P3.prune();
		return P3.retMap;
	}
}
