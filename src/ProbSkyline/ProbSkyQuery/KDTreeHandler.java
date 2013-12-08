package org.liang.ProbSkyQuery;

import org.liang.DataStructures.instance;
import org.liang.DataStructures.KDTreeInfo;

import org.liang.KDTree.*;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public class KDTreeHandler implements CompProbSky {

	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	/*
	 * input to this class is a list of instance.
	 */
	public List<instance> instList;
	public List<KDPoint> KDPList;

	public KDTree kdTree;
	public KDTreeInfo kdInfo;

	public HashMap<KDPoint, instance> KDMapInstance;
	public HashMap<Integer, Boolean> itemSkyBool;
	/**
	 * The left Corner point of range query.
	 */
	public KDPoint min;
	public KDPoint max;
	public int dim;

	public KDTreeHandler(List<instance> aList, int dim, HashMap<Integer, Boolean> itemSkyBool){
		this.dim = dim;	
		this.instList = aList;
		this.itemSkyBool = itemSkyBool;
	}

	void init( ){
		KDMapInstance = new HashMap<KDPoint, instance>();
		KDPList = new ArrayList<KDPoint> ();
		for(instance i: instList){
			KDPoint aKDPoint =  util.InstanceToKDPoint(i); 
			KDPList.add(aKDPoint);	
			KDMapInstance.put(aKDPoint, i);
		}
		createTree();
		min = new KDPoint(dim);
		min.setAllCoord(0.0);
		max = new KDPoint(dim);
		max.setAllCoord(1.0);
	}	

	void createTree(){
		
		if(PruneMain.verbose)
			log.info("dim =  " + dim);
		kdTree = new KDTree<Double>(Double.class, KDPList, dim);
		//System.out.println(kdTree.toString());

		if(PruneMain.verbose)
			log.info("num of Nodes =  " + kdTree.getNodeNum());
	
		kdInfo = new KDTreeInfo(KDMapInstance);
		kdInfo.setObjectBool(itemSkyBool);
	}

	void Traverse(){
		preOrder(kdTree.root);	
	}

	void preOrder(KDNode node){
		if(node == null) return;	
		computeInfo(node);
		preOrder(node.lesser);
		preOrder(node.greater);
	}

	@SuppressWarnings("unchecked")
	void computeInfo(KDNode node){
		
		if(node.parent == null){
			KDArea a_area = new KDArea(dim, min, max);
			kdInfo.init(node, a_area);	
		}	
		else{
			KDArea parentArea = node.parent.getArea();

			if(parentArea == null) System.out.println("Something Wrong happens parentArea here.");
			/*
			 * find the new area for range query. a_list is the instance list found in the range.
			 * currArea is the current node's area{ (0.0, 0.0), (min_x, min_y)}.
			 * parentArea is parent node's area{(0.0, 0.0), (min_x, min_y)}.
			 */
			KDArea currArea = node.getArea();
			if(PruneMain.verbose){
				if(node.getRL()){
					//log.info("KDArea currArea = "+ currArea.toString() );			
				}
			}
				
	    	if(currArea == null)
				System.out.println("Sth Wrong in currArea");

		    if(currArea.equals(parentArea)){
				//System.out.println("node String :" + node.toString());
				kdInfo.add(node, parentArea, null);
			}
			else{
				List<KDPoint> a_list = new ArrayList<KDPoint>();
				KDPoint max;

				if(!node.getRL()){
					kdTree.rangeQuery(kdTree.root, parentArea.max, currArea.max, a_list);

					/*
					 * old area stored for future search.
					 */
					max = ((KDRect)node).min;
					kdInfo.add(node, new KDArea(dim, min, max), a_list);
				}
				else{
					kdTree.rangeQuery(kdTree.root, parentArea.max, currArea.max, a_list);
					max = ((KDLeaf)node).point;
					kdInfo.add(node, new KDArea(dim, min, max), a_list);
				}
			}
		}
	}

	//@Override
	public void computeProb( ){
		
		this.init();
		this.createTree();
		this.Traverse();
	}
}
