package ProbSkyline;

import ProbSkyline.DataStructures.instance;
import mapreduce.ClusterConfig;

import java.util.ArrayList;

public class Util{

	public static int getObjectID(String objID){
		return Integer.parseInt(objID);        
	}

	public static int getInstID(String instID){
		return Integer.parseInt(instID);        
	}

	public static double getProb(String prob){
		return Double.parseDouble(prob);
	}
	
	/*
	 * getPoint(**,**) is designed for generating instances.
	 */
	public static double[] getPoint(String[] div, int dim){
		double [] ret = new double[dim];

		/**
		 * the data format is like this:.
		 *   ObjectID, InstanceID, double[0], ..., double[dim-1], prob 
		 */
		for(int i=0; i<dim; i++){
			ret[i] = Double.parseDouble(div[i+2]);        
		}
		return ret;
	}

	/*
	 * getPoint(**, **, **) is designed for generating instances in intermediate data.
	 */
	public static double[] getPoint(String[] div, int dim, boolean para){
		double [] ret = new double[dim];

		/**
		 * the data format is like this:.
		 *  areaID, L/R,  ObjectID, InstanceID, double[0], ..., double[dim-1], prob 
		 */
		for(int i=0; i<dim; i++){
			ret[i] = Double.parseDouble(div[i+4]);        
		}
		return ret;
	}


	public static int getPartition(instance aInst, ClusterConfig CC){
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
}
