package org.liang.ProbSkyQuery;

import java.io.File;

import org.liang.ClusterConfig;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.util.HashSet;

public class PruneNaiveMain{

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
		
		initializeLogger("NaiveProb");
		long tStart = System.currentTimeMillis();

		ClusterConfig CC = new ClusterConfig();
		Prune1And2 P12 = new Prune1And2(Integer.parseInt(CC.testArea));
		//P12.prune();

		NaivePrune NP = new NaivePrune(P12.itemsToItems());
		//NaivePrune NP = new NaivePrune();
		NP.prune();
		File theDir = new File("output");

		if (!theDir.exists()) {
			System.out.println("creating directory: ");
			boolean result = theDir.mkdir();  

			if(result) {    
				System.out.println("DIR created");  
			}
		}

		long tEnd = System.currentTimeMillis();
		long tDelta = tEnd - tStart;
		double elapsedSeconds = tDelta / 1000.0;
		System.out.println("elapsedSeconds : "+ elapsedSeconds );
	}	
}
