// Hyper-Rectangle class supporting KDTree class

package ProbSkyline.WrapRect;

import ProbSkyline.DataStructures.instance;

/*
 * Rect is the intermediate level of KDTree. It includes various
 *
 */
public class WRRect{

	/**
	 * Todo: because it is hard to identify the dimension number, 
	 * it is complicated to set a static min variable, that could have been set.
	 * If we knoe the dimension number at ClusterConfig class, we can set it well.
	 */
    public instance.point max;
	public instance.point min;

	public WRRect child = null;
	public WRRect parent = null;

    public int dim;

    public WRRect(int ndims) {
        dim = ndims;
    }

	public void init(){
        min = new instance.point(dim);
        max = new instance.point(dim);
	}

	public void set(instance.point min, instance.point max){
		this.min = min;		
		this.max = max;
	}

    public void setValue(double [] min_value, double [] max_value) {
			min.setPoint(min_value);
			max.setPoint(max_value);
    }


    public String toString() {
        return "A rectangle: " + min.toString() + "   " + max.toString() + "-----";
    }

    public int compareTo(WRRect o) {
        return 1;
    }

	public boolean DominateAnother(WRRect other){
		return this.max.DominateAnother(other.max);
	}

    public static void main(String args[]){
		
    }
}

