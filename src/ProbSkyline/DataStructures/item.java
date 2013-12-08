package org.liang.DataStructures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

public class item{

	public int objectID;
	public ArrayList<instance> instances;
	public instance.point min;
	public instance.point max;
	public int dim;
	public partialInfo PI;
	public boolean IfSkyline = true;
	public double objSkyProb;
	private static org.apache.log4j.Logger log = Logger.getRootLogger();
		
		
	public item (int objectID){
		this.objectID = objectID;
		instances = new ArrayList<instance>();
		dim = 0;
	}

	//public void CopypartialInfo(){
		//item ret = new item(this.objectID);
		//ret.
	
	//}

	public void addInstance(String[] parts, int a_dim){
		assert(parts.length == a_dim+3):"the dimension doesn't match";
		double[] temp = new double[a_dim];
		dim = a_dim;
		for(int i=0; i<a_dim; i++){
			temp[i] = Double.parseDouble(parts[i+2]);
		}
		this.addInstance(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), temp, Double.parseDouble( parts[parts.length-1]), a_dim);
	}

	public void setPartialInfo(int  a_min_area, int a_max_area){

		PI = new partialInfo();
		PI.setArea(a_min_area, a_max_area);
	}

	public void addInstance(int objectID, int instanceID, double[] coordinates, double prob, int a_dim){
		instance a_instance = new instance(instanceID, objectID, prob, a_dim);
		a_instance.setPoint(coordinates);
		this.instances.add(a_instance);
	}

	public void setMin(instance.point a_min){
//		log.info("test log 4j - - - " );
		this.min = a_min;
	}
			
	public void setMax(instance.point a_max){
		this.max = a_max;
	}

	public boolean PartitionJustify(int dimPartition, double location ){
		
		assert(dim !=0  && dimPartition<dim):"dim has not been assigned";
		double MaxAll=0; double MinAll = 0;
		for(int i=0; i<this.dim; i++){
			MaxAll += max.__coordinates[i];
			MinAll += min.__coordinates[i];
		}

		double min_value = min.__coordinates[dimPartition]/(MaxAll - max.__coordinates[dimPartition]+min.__coordinates[dimPartition] );
		double max_value = max.__coordinates[dimPartition]/(MinAll - min.__coordinates[dimPartition]+max.__coordinates[dimPartition] );

		if( (min_value<=location && max_value<=location) || (min_value>=location && max_value>=location  )  ) return true;
		else return false;
	}

	public boolean PartitionJustify(int dimPartition, double[] location ){
		
		assert(dim !=0  && dimPartition<dim):"dim has not been assigned";
		double MaxAll=0; double MinAll = 0;
		for(int i=0; i<this.dim; i++){
			MaxAll += max.__coordinates[i];
			MinAll += min.__coordinates[i];
		}

		double min_value = min.__coordinates[dimPartition]/(MaxAll - max.__coordinates[dimPartition]+min.__coordinates[dimPartition] );
		double max_value = max.__coordinates[dimPartition]/(MinAll - min.__coordinates[dimPartition]+max.__coordinates[dimPartition] );

		int len = location.length;
		for(int i=0; i<len; i++){

			if( (min_value<=location[i] && max_value<=location[i]) ) return true;

			if( (min_value<=location[i] && max_value > location[i]) ) return false;
			
		}
		return false;
	}
	
	@Override
	public boolean equals(Object other){
		if(other == null) return false;
		int otherObjID = ((item) other).objectID;	
		
		//System.out.println(" in equals this objectID = "+ this.objectID+"  other objectID = "+otherObjID  );

		if(otherObjID == this.objectID) return true;
		else return false;
	}



	public void Partition( double [] split, int splitNum, List< List<item> > splitItem ){
		
		double MaxAll=0; double MinAll = 0;
		for(int i=0; i<this.dim; i++){
			MaxAll += max.__coordinates[i];
			MinAll += min.__coordinates[i];
		}

		double min_value = min.__coordinates[0]/(MaxAll - max.__coordinates[0]+min.__coordinates[0] );
		double max_value = max.__coordinates[0]/(MinAll - min.__coordinates[0]+max.__coordinates[0] );
		
		int min_area =0; int max_area = 0;
		for(int i=splitNum-1; i>=0; i--){

			if( min_value<=split[i] )  {min_area = i; }

			if( max_value<=split[i] )  {max_area = i; }
		}
		this.setPartialInfo(min_area, max_area);	
		if(min_area == max_area) { 
			splitItem.get(min_area).add(new item(this.objectID));
		}
		else {
			splitItem.get(min_area).add(this);
			splitItem.get(max_area).add(this);
		}
	}

	public boolean JustifyDominate(item a_item){

		int i = 0;
		if( a_item.PI.max_area == a_item.PI.min_area  ){
			for(i=0; i<dim; i++){
				if(this.max.__coordinates[i] < a_item.min.__coordinates[i]) continue; 
			}
			if(i==dim) return true;	
		}
		return false;
	}


	public static class partialInfo{

	public ArrayList<instance> instances;

	public boolean Exist_min = false;
	public boolean Exist_max = false;

	public int min_area;
	public int max_area;
	partialInfo(){
		
	}
	
	void setArea(int a_min_area, int a_max_area ){

		min_area = a_min_area;
		max_area = a_max_area;
	}

	}
}
