package ProbSkyline;
import mapreduce.ClusterConfig;
import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;
import ProbSkyline.ProbSkyQuery.InstNaive;

import java.util.List;
import java.util.HashMap;

public class InterClient{

	ClusterConfig CC;
	instance aInst;

	public InterClient(ClusterConfig CC){
		this.CC= CC;	
	}

	public instance stringToInstance(String[] div){
		instance inst = null;
		if(div.length == CC.dim+5){
			inst= new instance( Util.getInstID(div[3]), Util.getObjectID(div[2]), Util.getProb(div[div.length-1]), CC.dim);
			inst.setPoint(Util.getPoint(div, CC.dim, true));
		}
		else
			System.out.println("Sth Wrong in creating instance.");

		return inst;
	}

	/**
	 * Based on the instance's position, its partition number is computed.
	 */	
	public int getPartition(String line){
		String [] div = line.split(" ");
		return Integer.parseInt(div[0]);
	}

	/**
	 * Do Prunning work in an partition.
	 */
	public HashMap<Integer, Double> comp(List<item> leftItemList, List<instance> rightInstList){
		InstNaive naive = new InstNaive(leftItemList, rightInstList);
		naive.computeProb();
		return null;
	}
}
