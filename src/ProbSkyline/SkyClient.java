package ProbSkyline;
import mapreduce.ClusterConfig;
import ProbSkyline.DataStructures.instance;


public class SkyClient{

	ClusterConfig CC;
	instance aInst;

	public SkyClient(ClusterConfig CC, String line){
		this.CC= CC;	
		String [] div = line.split(" ");
		if(div.length == CC.dim+3){
			aInst = new instance(Util.getObjectID(div[0]), Util.getInstID(div[1]), Util.getProb(div[div.length-1]), CC.dim);
			aInst.setPoint(Util.getPoint(div, CC.dim));
		}
		else
			System.out.println("Sth Wrong in creating instance.");
	}

	/**
	 * Based on the instance's position, its partition number is computed.
	 */	
	public int getPartition(){
		if(aInst != null)
			return aInst.partition(CC.splitValue, CC.numWorkers);	

		return -1;
	}
}
