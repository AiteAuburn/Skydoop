package org.liang.ProbSkyQuery;

import java.util.List;
import org.liang.Visual.*;

import org.liang.DataStructures.instance;
import org.liang.KDTree.KDPoint;

public class util{
	
	public static KDPoint InstanceToKDPoint(instance a_inst ){
		return 	PointToKDPoint(a_inst.a_point);
	}	

	public static KDPoint PointToKDPoint(instance.point point) {
		KDPoint ret = new KDPoint(point.dimCount);

		for(int i=0; i<point.dimCount; i++){
			ret.__coordinates[i] = point.__coordinates[i];	
		}	
		return ret;
	}

	public static void visual(List<instance> instList){
		
		new	InstVisualization(instList);
	}
}
