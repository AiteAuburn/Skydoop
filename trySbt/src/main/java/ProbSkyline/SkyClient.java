package ProbSkyline;
import mapreduce.ClusterConfig;
import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;

import ProbSkyline.ProbSkyQuery.Prune1And2;
import ProbSkyline.ProbSkyQuery.Prune3;

import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;

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
		else{
			System.out.println("div length = "+div.length + "  CC.dim = "+CC.dim);
			System.out.println("Here's Wrong in creating instance.");
		}
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
		else{
			System.out.println("div length = "+div.length + "  CC.dim = "+CC.dim);
			System.out.println("Here's Wrong in creating instance.");
		}
		return inst;
	}

	/**
	 * Based on the instance's position, its partition number is computed.
	 */	
	//public int getPartition(){
		//if(aInst != null)
			//return aInst.partition(CC.splitValue, CC.numWorkers);	

		//return -1;
	/*}*/

	/**
	 * Based on the angular partitioning, space is divided into even parts.
	 */
	public int getPartition(){
		if(aInst!= null){

			/*
			 * compute the length of the radius firstly.
			 */
			double r = 0.0;
			int dim = aInst.dimension;
			for(int i=0; i<dim; i++)
				r += aInst.a_point.__coordinates[i] * aInst.a_point.__coordinates[i];

			/*
			 * Detailed computation procedure could be referred in Angle-based Space Partitioning for Efficient Parallel
			 * Skyline Computation.
			 */
			double[] angle = new double[dim-1];
			for(int i=0; i<angle.length; i++){
				r -= aInst.a_point.__coordinates[i] * aInst.a_point.__coordinates[i];
				double tanPhi = Math.sqrt(r)/aInst.a_point.__coordinates[i];
				angle[i] = Math.atan(tanPhi);	
			}

			ArrayList< ArrayList<Double> > arrDouble = CC.arrDouble;

			/*
			 * Current partitioning scheme only supports two and three dimensional cases.
			 * if the returned value is -1, it denotes that sth wrong happened in the get partition number Process.
			 */
			if(dim == 2){

				ArrayList<Double> angles = arrDouble.get(0);
				for(int i=0; i<angles.size(); i++){
					if(angles.get(i) > angle[0])	
						return i;
				}		
				return -1;
			}
			else if(dim == 3){

				ArrayList<Double> anglesX = arrDouble.get(0);
				int partitionX = -1;
				for(int i=0; i<anglesX.size(); i++){
					if(anglesX.get(i) > angle[0])
						partitionX = i;
				}

				ArrayList<Double> anglesY = arrDouble.get(1);
				int partitionY = -1;
				for(int i=0; i<anglesY.size(); i++){
					if(anglesX.get(i) > angle[1])
						partitionY = i;
				}
				if(partitionX == -1 || partitionY == -1)
					return -1;
				else{
					int ret = partitionX * anglesX.size() + partitionY;
					return ret;
				}
			}
		}
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
