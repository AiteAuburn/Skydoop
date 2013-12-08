package ProbSkyline;
import mapreduce.ClusterConfig;
import ProbSkyline.DataStructures.instance;


public class SkyClient{

	ClusterConfig CC;
	instance aInst;


	public SkyClient(ClusterConfig CC, String line){
		this.CC = CC;	
		String [] div = line.split(" ");
		if(div.length == CC.dim+3){
			aInst = new instance(div);
		}
		else
			System.out.println("Sth Wrong in creating instance.");
	}

	/**
	 * Based on the instance's data, its partition number is computed.
	 */	
	public int getPartition(){
		
		
	}
}
