package org.liang.WrapRect;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.liang.DataStructures.instance;
import org.apache.log4j.Logger;

public class WRTreeInfo{

	private static org.apache.log4j.Logger log = Logger.getRootLogger();

	public HashMap<WRRect, Info> maintain;	
	public ArrayList<Integer> objectIDs;

	public Info originInfo = null;

	public final class Info{
		public HashMap<Integer, Double> theta;	
		public List<instance> instList;

		public Info(HashMap<Integer, Double> theta){
			this.theta = theta;
		}

		public void setList(List<instance> aList){
			instList = aList;
		}
	}

	public WRTreeInfo( ){
		maintain = new HashMap<WRRect, Info>();	
		objectIDs = new ArrayList<Integer>();
	}

	public void setObjectBool(HashMap<Integer, Boolean> ItemSkyBool ){

		Iterator iter_bool = ItemSkyBool.entrySet().iterator();
		while(iter_bool.hasNext()){
			Map.Entry obj_Bool = (Map.Entry) iter_bool.next();
			int id = (int) obj_Bool.getKey();
			if(ItemSkyBool.get(id) == true)	
				objectIDs.add(id);
		}
		//-------------------- the procedure init the originInfo after setting the object.
		init();
	}

	public void init(){
		HashMap<Integer, Double> theta = new HashMap<Integer, Double>();
		for(int i=0; i<objectIDs.size(); i++){
			int id = objectIDs.get(i);
			theta.put(id, 0.0);
		}
		
		originInfo = new Info(theta);
	}

	public HashMap<Integer, Double> copyInfo(HashMap<Integer, Double> org){
		HashMap<Integer, Double> theta = new HashMap<Integer, Double>();
		for(int i=0; i<objectIDs.size(); i++){
			int id = objectIDs.get(i);
			theta.put(id, org.get(id));
		}
		return theta;
	}

	public void initPartition(WRRect aWR, List<instance>a_list){

		System.out.println(aWR.toString() + "  " + a_list.size());

		HashMap<Integer, Double> theta;
		if(originInfo != null){
			theta = copyInfo(originInfo.theta);

			if(a_list != null){
				for(instance inst: a_list){
					int objectID = inst.objectID;
					double prob = inst.prob + theta.get(objectID);
					theta.put(objectID, prob);
				}
			}

			Info a_info = new Info(theta);
			a_info.setList(a_list);
			maintain.put(aWR, a_info);
		}
		else
			System.out.println("Sth Wrong in allocating init() in WRTreeInfo");
	}

	public List<instance> getInstList(WRRect aRect){

		Info aInfo = maintain.get(aRect);
		return aInfo.instList;
	}

	public HashMap<Integer, Double> getATheta(WRRect aRect){
		return copyInfo(maintain.get(aRect).theta);	
			
	}

	public void printInfo(Info aInfo){
		
		Iterator it = aInfo.theta.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry)it.next();	

			int objectID = (Integer)pairs.getKey();
			Double prob = (Double)pairs.getValue();

			log.info("objectID = "+ objectID+ " prob = "+prob);
		}
	}	

	public void iterateAllDiv(){

		Iterator it = maintain.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry)it.next();	

			WRRect aRect = (WRRect)pairs.getKey();

			Iterator it_2 = maintain.entrySet().iterator();
			while (it_2.hasNext()) {
				Map.Entry pairs_2 = (Map.Entry)it_2.next();	

				WRRect otherRect = (WRRect)pairs_2.getKey();
				if(aRect == otherRect) continue;

				if(aRect.DominateAnother(otherRect) == true){

					HashMap<Integer, Double> theta = maintain.get(otherRect).theta;

					HashMap<Integer, Double> addTheta = maintain.get(aRect).theta;

					Iterator it_3 = addTheta.entrySet().iterator();
					while (it_3.hasNext()) {
						Map.Entry pairs_3 = (Map.Entry)it_3.next();	
						if( (double)pairs_3.getValue() > 0){
							int objectID = (int)pairs_3.getKey();
							double prob = theta.get(objectID) + addTheta.get(objectID);
							theta.put(objectID, prob);
						}
					}
				}
			}
			log.info("new rectangle --------------------= " + aRect.toString());
			printInfo(maintain.get(aRect));
		}
	}

	public void iterateAllDiv(WRRect root){

		while(root.child != null){

			WRRect aRect = root.child;
			WRRect prev = root;

			HashMap<Integer, Double> theta = maintain.get(aRect).theta;

			HashMap<Integer, Double> addTheta = maintain.get(prev).theta;

			Iterator it_3 = addTheta.entrySet().iterator();
			while (it_3.hasNext()){
				Map.Entry pairs_3 = (Map.Entry)it_3.next();	
				if( (double)pairs_3.getValue() > 0){
					int objectID = (int)pairs_3.getKey();
					double prob = theta.get(objectID) + addTheta.get(objectID);
					theta.put(objectID, prob);
				}
			}

			log.info("new rectangle --------------------= " + aRect.toString());
			//printInfo(maintain.get(aRect));
			root = root.child;
		}
	}

	/* public void CompFinalSkyProb(HashMap<Integer, Double> theta, KDNode node){*/

	//KDPoint instPoint = ((KDLeaf) node).point;
		//instance aInst = KDMapInstance.get(instPoint);
		//int omitObjID = aInst.objectID;

		//double ret = 1.0;

		//Iterator it = theta.entrySet().iterator();
		//while (it.hasNext()) {
			//Map.Entry pairs = (Map.Entry)it.next();	
			//if( (Integer)pairs.getKey() == omitObjID) continue;

			//double objProb = (double)pairs.getValue();
			//if(objProb >= 1.0){
				//aInst.instSkyProb = 0.0;
				//return ;
			//}
			//else{

				////if(aInst.instanceID == 2805 && objProb>0){
				////log.info("itemID = "+ pairs.getKey()+ "   prob = " + objProb);
				////}
				//ret *= (1-objProb);	
			//}
		//}
		//aInst.instSkyProb = ret;
		//log.info("instance ID = "+ aInst.instanceID+ " prob = "+ret);
   /* }*/
}
