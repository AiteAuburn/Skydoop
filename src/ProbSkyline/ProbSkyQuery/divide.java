package dist;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import java.lang.Math;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Properties;
import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.Comparator;
import java.util.Collections;

import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.geom.Point2D;
import java.awt.geom.Ellipse2D;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JPanel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.*;
import com.romix.quickser.*;

import dist.*;

public class divide{
	
	public static final Logger log = LoggerFactory.getLogger(Prune.class);
	public static final boolean logEnable = true;
	public static final int testArea = 3;
	public static boolean[] objectPrune;
	public ArrayList<HashArea> HA;

	public List<item> listItem;
	public ArrayList<instance> instances;
	public static int dim;
	public static int MaxObjNum;

	public int pruneObjects = 0;
	public static double threshold = 0.1;
	public HyperPlane small;
	public HyperPlane large;

	divide( List<item> a_listItem, HyperPlane small, HyperPlane large){
		listItem = a_listItem;
		preprocess();	
		this.small = small;
		this.large = large;

		//readFile();
	}

	//public void readFile(){

		//try{
		//MyInputStream MIPS = new MyInputStream();
		//MIPS.setName(Integer.toString(testArea) +  ".txt");
		//MIPS.setDim(dim);
		//MIPS.read(listItem [> , hashItem <] );
		//MIPS.close();

		//FileInputStream input = new FileInputStream(new File("MAX_MIN"));
		//ObjectInputStream in = new ObjectInputStream(input);

		//outputLists = ( ArrayList< HashMap<Integer, HashMap<Integer, instance.point>> >)in.readObject();
		//in.close();
		//input.close(); 
		//}catch(ClassNotFoundException cnfe){ cnfe.printStackTrace();  }
		 //catch(IOException e){ e.printStackTrace(); }
		
		//if(logEnable){
			//System.out.println("the outputlists size = "+ outputLists.size() );
			//for(int i=0; i<outputLists.size() ;i++){
				//HashMap<Integer, HashMap<Integer, instance.point >> HM = outputLists.get(i);				
				//Iterator iter = HM.entrySet().iterator();
				//while(iter.hasNext()){
					//Map.Entry obj = (Map.Entry) iter.next();
					////System.out.println("The map key = "+obj.getKey() );
					//HashMap<Integer, instance.point> mapPoint =(HashMap<Integer, instance.point>) obj.getValue();
					
					//Iterator iter_2 = mapPoint.entrySet().iterator();
					//while(iter_2.hasNext()){
						//Map.Entry obj_2 = (Map.Entry) iter_2.next();
						////if( (int) obj.getKey()==0)
						//System.out.println("The object id = "+obj_2.getKey()+"   "+ ((instance.point)obj_2.getValue()).toString()  );
							
					//}

				//}
			//}
		//}
		//}

	public int findLoc(point hyperPoint){
		double sum = hyperPoint.totalValue();
	
		for(int i=0; i<HA.size(); i++){
			HashArea curr = HA.get(i);
			if(sum<= curr.divideValue )
				return i;
			else
				System.out.println("sum = "+ sum + " curr.divideValue = " + curr.divideValue );
		}	
		System.out.println("Something wroing in findLoc Function");
		return 999;
	}

	class SortComparator implements Comparator<instance> {

		@Override
		public int compare(instance a, instance b){

			double a_total = a.a_point.sum();
			double b_total = b.a_point.sum();

			if(a_total - b_total>0) return 1;
			else return -1;
		}		
	}

	public void compProb(int n){
	
	final long startTime = System.currentTimeMillis();
		int size = instances.size();
		System.out.println("size = "+ size);

		Collections.sort(instances, new SortComparator());

//--------------------------initialize data structures for cal prob---------------
	
		ArrayList<List<instance>> divide = new ArrayList< List<instance> > (n);
		HA = new ArrayList<HashArea> (n);
		for(int i=0; i<n; i++){
			List<instance> oneNth = new ArrayList<instance>();
		
			int index_begin = (int) Math.ceil( ((double)(i)) / ((double)n )*size     );
			int index_end = (int) Math.ceil( ((double)(i+1)) / ((double)n )*size     );
			System.out.println("index_begin  = "+ index_begin+ " index_end "+index_end );

			oneNth = /*( ArrayList<instance> )*/ instances.subList(index_begin, index_end); 
				
			double divideValue = instances.get( index_end - 1 ).a_point.sum();
			divide.add(oneNth);
			HashArea curr = new HashArea(i);
			curr.setMaxValue(divideValue);
			HA.add(curr);
		}

//-----------------------compute base additions-----
		
		baseComp(divide, HA);

		double count_line = 0.0;
//-----------------------local skyline computation---
		for(int i=0; i<instances.size(); i++){
			
			instance curr = instances.get(i);	
			
			if(curr.IfSkyline == false) { 
				curr.instSkyProb = 0.0;
				continue;	
			}

			if(  ( (double)i/(double)instances.size() ) >  count_line ){
				System.out.println("instance percent =  " + (double)i/(double)instances.size()   );
				count_line += 0.01;
			}
			point a_point = new point(dim);
			a_point.setValue(curr.a_point.__coordinates);

			point [] ret = a_point.ReturnNPoints(small,large);

			int min_area = 999;
			int max_area = 0;
			for(int j=0; j<ret.length; j++){
				int loc = findLoc(ret[j]);	
				if(loc<=min_area) min_area = loc;
				if(loc>=max_area) max_area = loc;
			}
			
			//System.out.println("min_area = "+ min_area +"  max_area =  "+max_area  );
			HashArea comp = new HashArea(-1);
			comp.preprocess();	
			
			for(int j=min_area; j<=max_area; j++){
				List<instance> oneNth = divide.get(j);
				for(int k=0; k<oneNth.size(); k++){
					instance dominator = oneNth.get(k);
					if(dominator.a_point.DominateAnother(curr.a_point)){
						comp.add(dominator.objectID,dominator.prob );	
					}
					
				}
			}

			for(int j =0; j<=min_area-1; j++){
				
				comp.addHashObject(HA.get(j));	
			}
			curr.instSkyProb = comp.computeInstSkyProb(curr.objectID);
			//if(curr.instSkyProb >0.01)
			//System.out.println("local skyline prob = " + curr.instSkyProb  );
		}

		for(int i=0; i<listItem.size(); i++){
			item curr = listItem.get(i);
			curr.objSkyProb = 0.0;
			for(int j=0; j<curr.instances.size(); j++){
				instance currInst = curr.instances.get(j);
				curr.objSkyProb = currInst.prob * currInst.instSkyProb;
			}
			if(curr.objSkyProb < threshold) pruneObjects ++;
		}
		final long endTime = System.currentTimeMillis();

		System.out.println("prunObjects num = "+pruneObjects );
		System.out.println("Total execution time: " + (endTime - startTime) );
	}

	public void computeNaiveProb(){

		final long startTime = System.currentTimeMillis();

		Collections.sort(instances, new SortComparator());

		double count_line = 0.0;
		for(int i=0; i<instances.size(); i++){
			
			instance curr = instances.get(i);	
			if(  ( (double)i/(double)instances.size() ) >  count_line ){
				System.out.println("instance percent =  " + (double)i/(double)instances.size()   );
				count_line += 0.01;
			}
			point a_point = new point(dim);

			HashArea comp = new HashArea(-1);
			comp.preprocess();	
			
			for(int k=0; k<instances.size(); k++){
				instance dominator = instances.get(k);
				if(dominator.instanceID == curr.instanceID) continue;
				if(dominator.sum(dominator.a_point) > curr.sum(curr.a_point) ) continue;
					if(dominator.a_point.DominateAnother(curr.a_point)){
						comp.add(dominator.objectID,dominator.prob );	
					}
					
				}

			curr.instSkyProb = comp.computeInstSkyProb(curr.objectID);
			//if(curr.instSkyProb >0.01)
			//System.out.println("local skyline prob = " + curr.instSkyProb  );
		}

		testPrune3();
		final long endTime = System.currentTimeMillis();

		System.out.println("Total execution time: " + (endTime - startTime) );
	}

	public void testPrune3(){

		for(int i=0; i<listItem.size(); i++){
			item curr = listItem.get(i);
			curr.objSkyProb = 0.0;
			for(int j=0; j<curr.instances.size(); j++){
				instance currInst = curr.instances.get(j);
				curr.objSkyProb = currInst.prob * currInst.instSkyProb;
			}

			if(curr.objSkyProb < threshold) pruneObjects ++;
		}
		System.out.println("prunObjects num = "+pruneObjects );
	}

	public void baseComp(ArrayList< List<instance> > divide, ArrayList<HashArea> HA ){
		
		for(int i=0; i<divide.size(); i++){
		
			System.out.println("working on no area = "+ i );	
			List<instance> curr = divide.get(i);	
			System.out.println("curr size =  "+ curr.size() );

			HashArea currHash = HA.get(i);

			if(i>0) currHash.cloneHashMap( HA.get(i-1) );
			else currHash.preprocess();

			for(int j=0; j<curr.size(); j++){
			
				int key = curr.get(j).objectID;
				double prob = curr.get(j).prob;
				//if(currHash.hashObject.get(key) >0 )
				//System.out.println(key +"  "+ currHash.hashObject.get(key) );
				currHash.add(key,prob);
			}
			HA.add(currHash);	
		}	
	}


	public void preprocess(){

		Properties prop = new Properties();
		instances = new ArrayList<instance>();
		
		try {
			prop.load(new FileInputStream("config.properties"));;

		}catch (IOException ex) {
			ex.printStackTrace();
		}
		MaxObjNum = Integer.parseInt(prop.getProperty("objectNum"));
		dim = Integer.parseInt(prop.getProperty("dim"));
		objectPrune = new boolean[MaxObjNum];

		for(int i=0; i<listItem.size(); i++){
			instances.addAll(listItem.get(i).instances);
			
		}
	}


	public static final class HashArea{
		
		int num;
		int area_id;
		HashMap<Integer,Double> hashObject;
		double divideValue;

		public HashArea( int a_area){

			assert(divide.MaxObjNum != 0 ): "object number has not been initialized.";
			num = divide.MaxObjNum;
			area_id = a_area;
			hashObject = new HashMap<Integer, Double> ();
			//preprocess();
		}

		public void setMaxValue(double divideValue){
			
			this.divideValue = divideValue;	
		}

		public void preprocess(){

			for (int i=0; i<num; i++){
				hashObject.put(i,0.0);	
			}	
		}
	
		public void add(int key, double a_prob){
			
			double currProb = (Double) hashObject.get(key);
			hashObject.put(key, currProb+a_prob );
		}

		public void addHashObject(HashArea a_HashArea){
			
			for(int i=0; i<num; i++){
				double value  = (Double) a_HashArea.hashObject.get(i);
				this.add(i,value);
			}
		}

		public double computeInstSkyProb(int objectID){
		
			double ret = 1.0;
			for(int i=0; i<num; i++){
				double curr = (Double) this.hashObject.get(i);
				if(curr > 0 && i!=objectID ) ret *= curr;
			}
			return ret;
		}

		//@Override
		//protected Object clone() throws CloneNotSupportedException {
			//HashArea cloned = new HashArea(this.area_id+1);
			//cloned.preprocess();
			//return cloned;
		//}
		
		public void cloneHashMap(HashArea a_hash){
			
			this.hashObject	= (HashMap<Integer, Double>)a_hash.hashObject.clone();
		}
	}


	public static void main(String[] args){
		
		//divide dv = new divide();
	}
}
