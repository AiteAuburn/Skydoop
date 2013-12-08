package org.liang.ProbSkyQuery;

import org.liang.ClusterConfig;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Merge{

	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	public static boolean verbose = true;
	
	public static void initializeLogger(String fileName){
		FileAppender fa = new FileAppender();
		fa.setName("FileLogger");
		fa.setFile(fileName+ ".log");
		fa.setLayout(new PatternLayout("%d{HH:mm:ss}  %m%n  "));
		fa.setThreshold(Level.INFO);
		fa.setAppend(false);
		fa.activateOptions();
		log.addAppender(fa);
	}

	public static void main(String [] args){
		
		initializeLogger("Merge");
		ClusterConfig conf = new ClusterConfig();

		System.out.println("test numworkers = "+ conf.numWorkers);
		
		long tStart = System.currentTimeMillis();

		Prune1And2 P12 = new Prune1And2();
		P12.prune();

		NaivePrune NP = new NaivePrune(P12.itemsToItems());
		//NaivePrune NP = new NaivePrune();
		NP.prune();

		long tEnd = System.currentTimeMillis();
		long tDelta = tEnd - tStart;
		double elapsedSeconds = tDelta / 1000.0;
		System.out.println("elapsedSeconds : "+ elapsedSeconds );
	}	
}
