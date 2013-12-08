package ProbSkyline;


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

	public static double[] getPoint(String[] div, int dim){
		double [] ret = new double[dim];

		/**
		 * the data format is like this:.
		 *    ObjectID, InstanceID, double[0], ..., double[dim-1], prob 
		 */
		for(int i=0; i<dim; i++){
			ret[i] = Double.parseDouble(div[i+2]);	
		}
		return ret;
	}

}
