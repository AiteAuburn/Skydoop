package org.liang.ProbSkyQuery;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import org.liang.Visual.InstVisualization;
import org.liang.ClusterConfig;

/**
 * Main function of prune 1.1-1.3.
 */

public class PruneMain{

	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	public static boolean verbose = true;
	
	public static void initializeLogger(String fileName)
	{
		FileAppender fa = new FileAppender();
		fa.setName("FileLogger");
		fa.setFile(fileName+ ".log");
		fa.setLayout(new PatternLayout("%d{HH:mm:ss}  %m%n  "));
		fa.setThreshold(Level.INFO);
		fa.setAppend(true);
		fa.activateOptions();
		log.addAppender(fa);
	}

	public static void main(String [] args){

		initializeLogger("WRProb");
		long tStart = System.currentTimeMillis();
		ClusterConfig CC = new ClusterConfig();
		Prune3 P3 = new Prune3();	
		P3.setClusterConfig(CC);
		//P12.preprocess();
		P3.prune();

	   /* Prune1And2 P12 = new Prune1And2(5);*/
		//P12.prune();

		//new InstVisualization(P12.itemsToinstances());

		//new InstVisualization(WRTreeHandler.medList,true);

		long tEnd = System.currentTimeMillis();
		long tDelta = tEnd - tStart;
		double elapsedSeconds = tDelta / 1000.0;
		System.out.println("epasedSecond :"+ elapsedSeconds );
	}	
}
