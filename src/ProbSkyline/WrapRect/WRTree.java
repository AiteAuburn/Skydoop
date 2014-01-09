package ProbSkyline.WrapRect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import ProbSkyline.DataStructures.*;

/**
 *	Wrap Rectangle tree;
 */
@SuppressWarnings("rawtypes")
public class WRTree{

    private int k;
	public static int nodeNum = 0;
	public static instance.point min;
	public List<WRRect> RectList;
	public WRRect root;
    private ALLCOMPARATOR A_COMPARATOR;

    final class ALLCOMPARATOR implements Comparator<instance.point>{

        private final boolean ascending;

        public ALLCOMPARATOR(){
            this.ascending = true;
        }
        public int compare(instance.point o1, instance.point o2) {
            return o1.compareTo(o2);
        }
    }

    /**
     * Default constructor.
     */
	public WRTree() {
		this.k = 2;
		this.A_COMPARATOR = new ALLCOMPARATOR();
	}

    /**
     * More efficient constructor.
     *
     * @param list of XYZPoints.
     */

    public WRTree(List<instance.point> list, int a_dim) {
        this.k = a_dim;
        this.A_COMPARATOR = new ALLCOMPARATOR();
		this.RectList = new ArrayList<WRRect>();
		min = new instance.point(k);

		min.setOneValue(0.0);
        createRect(list);
    }


	public int getNodeNum(){
		
		return nodeNum;	
	}


	public int compPosition(instance.point aPoint){
		
		for(int i=RectList.size()-1; i>=0; i--){
		
			instance.point max = RectList.get(i).max;	
			if( A_COMPARATOR.compare(aPoint,max) > 0)
				continue;
			else
				return i+1;
		}	
		return 0;
	}

	public WRRect compNearRect(instance aInst){
	
		int i=0;	
		for(i=0; i<RectList.size(); i++){
		
			instance.point max = RectList.get(i).max;	
			//System.out.println("max = "+ max.toString() + "  aInst = "+aInst.toString());
			if( A_COMPARATOR.compare(max, aInst.a_point) > 0)
				continue;
			else
				break;
		}	
		i = i-1;
		if(i < 0) return null;
		return RectList.get(i);
	}

	public WRRect compStopRect(instance aInst){

		int i=0;	
		for(i=RectList.size()-1; i>=0; i--){

			instance.point max = RectList.get(i).max;	
			if( A_COMPARATOR.compare(aInst.a_point, max) > 0)
				continue;
			else
				break;
		}	

		return RectList.get(i+1);
	}

	/**
	 * Create node from list of XYZPoints.
	 *
	 * @param list of XYZPoints.
	 */
	public void createRect(List<instance.point> list) {

		WRRect previous = null;
		//------create the bottem left rectangle.
		if(list.size() > 1){
			root =new WRRect(k);
			previous = root;
			root.set(min, list.get(0));
			RectList.add(root);
		}
		else
			System.out.println("too few div number");

		for(int i=1; i<list.size(); i++){
			WRRect aWRRect = new WRRect(k);
			aWRRect.set(min, list.get(i));
			aWRRect.parent = previous; 
			previous.child = aWRRect;
			RectList.add(aWRRect);
			previous = aWRRect;
		}
		/*
		 * create the topRight rectangle
		 */
		WRRect aWRRect = new WRRect(k);
		instance.point max = new instance.point(k);
		max.setOneValue(1.0);
		aWRRect.set(min, max);
		aWRRect.parent = previous; 
		previous.child = aWRRect;
		RectList.add(aWRRect);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			for(int i=0; i<RectList.size(); i++){

				builder.append(RectList.get(i).toString()).append("    ");		
			}
			return builder.toString();
		}


	public static List<instance.point> generatePoints( int div ,int dim, instance.point extreme) {
		List<instance.point> list = new ArrayList<instance.point>();

		double aNum = 1.0/ ((double)div+1.0 );

		double temp = aNum;
		for(int i = 0; i < div; ++i) {
			instance.point aPoint = new instance.point(dim);
			aPoint.setOneValue(extreme, aNum);
			list.add(aPoint);
			aNum = aNum+ temp;
		}
		return list;
	}


	@SuppressWarnings("unchecked")
		public static void main (String args[]){

			//WRTree aWRTree = new WRTree(aList, 3);

			//System.out.println( aWRTree.toString());
		}
}
