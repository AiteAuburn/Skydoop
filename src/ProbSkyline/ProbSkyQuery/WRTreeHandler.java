package ProbSkyline.ProbSkyQuery;

import ProbSkyline.DataStructures.instance;

import ProbSkyline.WrapRect.*;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public class WRTreeHandler implements CompProbSky {

	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	/*
	 * input to this class is a list of instance.
	 */
	public List<instance> instList;
	public List< List<instance> > divList; 
	public static List<instance.point> medList; 
	public instance.point extreme;

	public WRTree wrTree;
	public WRTreeInfo wrTreeInfo;
	private int dim;
	//-----------------div ranges from 0 to div(inclusive, inclusive)
	public int div;

	public HashMap<Integer, Boolean> itemSkyBool;
	/**
	 * The left Corner point of range query.
	 */

	public WRTreeHandler(List<instance> instList, int dim, HashMap<Integer, Boolean> itemSkyBool, int div){
		this.instList = instList;
		this.dim = dim;	
		this.itemSkyBool = itemSkyBool;
		this.div = div;
		this.divList = new ArrayList< List<instance>>();
		for(int i=0; i<= div; i++){
			divList.add(new ArrayList<instance>());	
		}
		medList = new ArrayList<instance.point>();
	}

	public void findExtreme(){
		extreme = new instance.point(dim);
		instance.point aPoint = new instance.point(dim);
		double[] max = new double[dim];
		for(int i =0; i<max.length; i++){
			max[i] = 0.0;	
		}
		for(int i=0; i<instList.size(); i++){

			instance inst = instList.get(i);
			for(int j=0; j<dim; j++){
				if(inst.a_point.__coordinates[j] > max[j])
					max[j] = inst.a_point.__coordinates[j];
			}
		}
		extreme.setPoint(max);
	}


	void createTree(){
		if(PruneMain.verbose)
			log.info("dim =  " + dim);

		/**
		 * It contains several procedures to create the wrap rectangle tree:
		 * 1, find the extreme value of all instances in the partition;
		 * 2, generate points, which are the top right point of every rectangle, based on the extreme value;
		 * 3, construction for WRTreeInfo;
		 * 4, find unique objectids which still necessary for computation.
		 */

		findExtreme();
		medList = WRTree.generatePoints(this.div, this.dim, extreme);
		wrTree = new WRTree(medList, this.dim);
		wrTreeInfo = new WRTreeInfo();
		wrTreeInfo.setObjectBool(itemSkyBool);
		System.out.println(wrTree.toString());
	}

   /* public void computeCenterPoint(){*/
		
		//for(int i=0; i<instList.size(); i++){
			
			//int index = wrTree.compPosition(instList.get(i).a_point);	
			//divList.get(index).add(instList.get(i));
		//}

		//for(int i=0; i<divList.size(); i++){
			
			//List<instance> part = divList.get(i);	
			//instance.point medPoint = compMedian(part);
			//medList.add(medPoint);
			//part.clear();
		//}
	//}

	
	//public instance.point compMedian(List<instance> part){
		
		//instance.point medPoint = new instance.point(dim);	
		//medPoint.setOneValue(0.0);
		//for(int i=0; i<part.size(); i++){
			//instance curr = part.get(i);	

			//for(int j=0; j<dim; j++){
				//medPoint.__coordinates[j] += curr.a_point.__coordinates[j];
			//}
		//}

		//for(int j=0; j<dim; j++){
			//medPoint.__coordinates[j] = medPoint.__coordinates[j]/part.size(); 
		//}
		//return medPoint;
	/*}*/


	void assign(instance aInst){

		//----div paritions increases from 6 to 7 .
		int index = 0;
		for(int i=0; i<medList.size(); i++){
			
			instance.point pt = medList.get(i);	
			if( aInst.a_point.compareTo(pt) >0 ){
				break;
			}
			index++;		
		}
		divList.get(index).add(aInst);
	}

	void findAllPartition(){
		for(int i=0; i<instList.size(); i++){
			assign(instList.get(i));
		}
	}

	@SuppressWarnings("unchecked")
	void computeInfo(){
		WRRect start = null;
		for(int i=0; i<wrTree.RectList.size(); i++){

			wrTreeInfo.initPartition(wrTree.RectList.get(i), divList.get(i));	
		}
		
		wrTreeInfo.iterateAllDiv(wrTree.root);

		System.out.println("instance size = "+ instList.size());
		/**
		 * For every instance, we compute its skyline probabilities:
		 * 1, we first compute the leftbottom rectangle, which fully dominate the instance, and the upper right rectangle which can not 		   dominate the instance;
		 * 2, Then we copy the Hashmap from bottom left rectangel, and aggreate value at the current rectangle, where some instances dom		   inate curr;
		 * 3, compute the skyline probability based on the formula.
		 */

		for(int i=0; i<instList.size(); i++){

			instance curr = instList.get(i);

			WRRect nearLeftBottemRect = wrTree.compNearRect(curr);
			WRRect stopRect = wrTree.compStopRect(curr);
			if(stopRect == null)
				System.out.println("Sth wtrong in deciding the boundary rectangle!");

			HashMap<Integer, Double> theta = null;
			if(nearLeftBottemRect !=null){
				theta = wrTreeInfo.getATheta(nearLeftBottemRect);
				start = nearLeftBottemRect.child;

			}
			else{
				start = wrTree.root;
				//System.out.println("----start root-="+ start.toString());
				//log.info("   start expanding = "+ start.toString());
				if(wrTreeInfo.originInfo != null)
					theta = wrTreeInfo.copyInfo(wrTreeInfo.originInfo.theta);
				else
					System.out.println("Something wrong in allocaltion originInfo in wrTreeInfo. ");

			}
			//log.info("   stopRect= "+ stopRect.toString());

			do{
   /*             System.out.println("start = "+ start.toString());*/
				//System.out.println("stop = "+ stopRect.toString());
				/*System.out.println("curr = "+ curr.toString());*/
				List<instance> instList = wrTreeInfo.getInstList(start);
				for(instance inst: instList){
					int objectID = inst.objectID;
					if(inst.DominateAnother(curr) ){
						if(theta.containsKey(objectID) ){
							double prob = inst.prob + theta.get(objectID);
							theta.put(objectID, prob);
						}
						else{
							theta.put(objectID, inst.prob);
						}
					}
				}
				start = start.child;
			}while(start != stopRect.child);

			if(theta == null)
				System.out.println("Sth wtrong in allocating theta in WRTREEHandler!");

			Iterator it = theta.entrySet().iterator();
			double skyProb = 1.0;
			while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry)it.next();
				double aValue = (double) pairs.getValue();
				int objID = (int) pairs.getKey();
				if(objID != curr.objectID)
					skyProb *= (1.0 - aValue);
			}
			curr.instSkyProb = skyProb;
			//System.out.println("curr ID = "+ curr.instanceID + " instSkyProb = "+ skyProb);
		}
	}


		@Override
			public void computeProb( ){

				this.createTree();
				this.findAllPartition();
				this.computeInfo();
			}
	}
