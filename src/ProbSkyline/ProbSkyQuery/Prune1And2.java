package ProbSkyline.ProbSkyQuery;

import mapreduce.ClusterConfig;
import ProbSkyline.DataStructures.*;
import ProbSkyline.IO.*;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Properties;
import java.util.Iterator;
import java.util.Map;

import ProbSkyline.Visual.MinMaxVisual;

public class Prune1And2 extends PruneBase{

	public List<item> afterPrune1;
	public List<instance> instances;

	public Prune1And2 (List<item> itemList, ClusterConfig CC){
		super(itemList, CC);
		this.preprocess();
	}

	@Override
	public void preprocess() {
		super.readFile();
		super.setItemSkyBool();
		System.out.println("before Prune 1 the number of items  = "+ listItem.size());
	}

	/**
	 * convert items to instance List for wrap Rectangle Tree.
	 */
	public List<instance> itemsToinstances(){
		int instanceAfter = 0;
		instances = new ArrayList<instance>();

		for(int i=0; i<listItem.size(); i++){
			item aItem = listItem.get(i);
			if(ItemSkyBool.get(aItem.objectID) == false )
				continue;
				
			for(int j=0; j<aItem.instances.size();j++){
				instanceAfter++;
				instances.add(aItem.instances.get(j));
				//System.out.println("inst ID = "+ aItem.instances.get(j).instanceID);
			}
		}
		System.out.println("after Prune 1 the number of instances = "+ instanceAfter);
		return instances;
	}

	/**
	 * Prune objects which is pruned in the phase prune 1.1
	 */
	public List<item> itemsToItems(){

		int itemAfter = 0;
		List<item> retList= new ArrayList<item>();

		for(int i=0; i<listItem.size(); i++){
			item aItem = listItem.get(i);
			if(ItemSkyBool.get(aItem.objectID) == false )
				continue;
			
			itemAfter++; 
			retList.add(aItem);				
		}
		System.out.println("after Prune 1 the number of items= "+ itemAfter);
		return retList;
	}


	public void prune(){
		rule1();
		setAfterPrune1List();
		rule2();
	}

	@SuppressWarnings("rawtypes")
	public void setAfterPrune1List(){
	
		Iterator iter_bool = ItemSkyBool.entrySet().iterator();
		while(iter_bool.hasNext()){
			Map.Entry obj_Bool = (Map.Entry) iter_bool.next();	
			int id = (int) obj_Bool.getKey();
			if( ItemSkyBool.get(id)== false)
				super.corrIndex.remove(id);	
		}
	}

	public void VisualMinMax(HashMap<Integer, instance.point> min, HashMap<Integer, instance.point> max){

		List<instance.point> minList = new ArrayList<instance.point>();
		List<instance.point> maxList = new ArrayList<instance.point>();

		Iterator iter_max = max.entrySet().iterator();
		while(iter_max.hasNext()){
			Map.Entry obj_max = (Map.Entry) iter_max.next();	
			int objectID= (int) obj_max.getKey();
			//if( !min.containsKey(objectID))
				//System.out.println("Something got wrong in unbalance between min and max");
			
			//minList.add( min.get(objectID)  );
			maxList.add( max.get(objectID)  );
		}

		Iterator iter_min = min.entrySet().iterator();
		while(iter_min.hasNext()){
			Map.Entry obj_min = (Map.Entry) iter_min.next();	
			int objectID= (int) obj_min.getKey();
			//if( !min.containsKey(objectID))
				//System.out.println("Something got wrong in unbalance between min and max");
			
			minList.add( min.get(objectID)  );
			//maxList.add( max.get(objectID)  );
		}

		new MinMaxVisual(minList, maxList);
	}


	@SuppressWarnings("rawtypes")
	public void rule1(){
		
		PartitionInfo thisArea = outputLists.get(Integer.parseInt(CC.testArea));

		if(thisArea != null){
			HashMap<Integer, instance.point> min = thisArea.min;
			HashMap<Integer, instance.point> max = thisArea.max;

			/*
			 * visualized Min Max rectangle by calling this function.
			 */
			//VisuaMinMax(min, max);

			Iterator iter_max = max.entrySet().iterator();
			while(iter_max.hasNext()){
				Map.Entry obj_max = (Map.Entry) iter_max.next();	

				int max_id = (int) obj_max.getKey();
				
				/*
				 * if ItemSkyBool hasn't the key, it means that a object's max corner is in this partition, but all objects
				 * don't  appear in the partition. Or, the objects has been pruned since it is false.
				 */
				if(  ItemSkyBool.get(max_id) == null ||  (Boolean)ItemSkyBool.get(max_id)== false )  continue;

				instance.point max_value = (instance.point)obj_max.getValue();	

				Iterator iter_min = min.entrySet().iterator();

				//System.out.println(" max = "   +max_value.toString());
				while(iter_min.hasNext()){
					Map.Entry obj_min = (Map.Entry) iter_min.next();	
					int min_id = (int)obj_min.getKey();

					/*
					 * if ItemSkyBool hasn't the key, it means that a object's min corner is in this partition, but all objects 				 
					 * don't doesn't appear in the partition.
					 */
					instance.point min_value = (instance.point)obj_min.getValue();	
					//System.out.println("      min = "   + min_value.toString());
					if(  ItemSkyBool.get(min_id) == null || min_id==max_id || (Boolean)ItemSkyBool.get(min_id) == false )   continue;

					if(max_value.DominateAnother(min_value)) {
						ItemSkyBool.put(min_id, false);
						//System.out.println(" max  dominate min" );
					}
				}
			}
		}
	}


	@SuppressWarnings("rawtypes")
	public void rule2(){

		PartitionInfo thisArea = outputLists.get(Integer.parseInt(CC.testArea));
		
		Iterator iter_corr = corrIndex.entrySet().iterator();
		while(iter_corr.hasNext()){
			Map.Entry obj_corr = (Map.Entry) iter_corr.next();	
			int id = (int) obj_corr.getKey();
			int indexInList = corrIndex.get(id);
				
			item iterItem = listItem.get(indexInList);	
			for( int j=0; j<iterItem.instances.size(); j++ ){	
				instance currInst = iterItem.instances.get(j);

				Iterator iter_corr_2 = corrIndex.entrySet().iterator();
				while(iter_corr_2.hasNext()){
					Map.Entry obj_corr_2 = (Map.Entry) iter_corr_2.next();	
					int objectid = (int) obj_corr_2.getKey();

					if( thisArea.max.containsKey(objectid ) ){
					
						if(thisArea.max.get(objectid).DominateAnother(currInst.a_point) )
							currInst.IfSkyline = false;
					}
				}	
			}
		}
	}
}
