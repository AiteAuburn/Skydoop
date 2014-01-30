package ProbSkyline;
import mapreduce.ClusterConfig;
import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;
import ProbSkyline.Visual.InstVisualization;
import ProbSkyline.ProbSkyQuery.InstNaive;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collections;
import java.util.Comparator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;

/**
 * This class is in charge of the partitioning after first phase of MapReduce.
 */
public class Intermediate{
	ClusterConfig CC;
	String inputPath;
	List<item> itemList;
	List<instance> instList;
	List<instance> leftInstList;
	ArrayList<ArrayList<instance>> divList;
	Set<Integer> cands;

	public Intermediate(ClusterConfig CC, String inputPath){
		this.CC=CC;	
		cands = new HashSet<Integer>();
		this.inputPath = inputPath;
	}

	public String getSrcInstanceFile(){
		return CC.srcName;	
	}


	public instance stringToInstance(String instString){

		String [] div = instString.split(" ");
		instance inst = null;
		if(div.length == CC.dim+3){

			inst= new instance(Util.getInstID(div[1]), Util.getObjectID(div[0]), Util.getProb(div[div.length-1]), CC.dim);
			inst.setPoint(Util.getPoint(div, CC.dim));
		}
		else
			System.out.println("Sth Wrong in creating instance.");

		return inst;
	}


	public void getItemList(String inputFile){

		SkyClient client = new SkyClient(ClusterConfig.getInstance());
		HashMap<Integer, item> aMap = new HashMap<Integer, item>();

		try{
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line = br.readLine();
			while( line != null){

				instance curr = stringToInstance(line);
				int objectID = curr.objectID;
				if(aMap.containsKey((Integer)objectID) == false){

					item aItem = new item(objectID);         
					aMap.put(objectID, aItem);
				}
				item currItem = aMap.get(objectID);
				currItem.addInstance(curr);
				line = br.readLine();
			}
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
		this.itemList = new ArrayList<item>(aMap.values());
	}

	/*
	 * Remove all items which is unrelated to candidates in the next phase.
	 */
	public void readCandidates(){
		File f = new File(this.inputPath);	
		if(f.isDirectory()){
			File[] files = f.listFiles();	
			for(File file: files)
				readCandidates(file);	
		}	
	}

	public void readCandidates(File file){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while( line != null){
				String[] oneLine = line.split("\t");
				cands.add( Integer.parseInt(oneLine[0]));
				line = br.readLine();
			}
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}

	}

	void removeUnrelatedObjects(){

		System.out.println("Before removing unrelated object the num is "+ itemList.size());
		Iterator it = itemList.iterator();
		while(it.hasNext()){
			item obj = (item)it.next();
			if(cands.contains(obj.objectID) == false)
				it.remove();
		}
		System.out.println("Finish removing, Now, the num is "+ itemList.size());
	}
	
	/*
	 * Create an instance list to store all instances as a list to help partition.
	 */
	public void itemsToInstances(){
		this.instList = new ArrayList<instance>();

		for(int i=0; i<itemList.size(); i++){
			item aItem = itemList.get(i);
			for(int j=0; j<aItem.instances.size();j++){
				instList.add(aItem.instances.get(j));
			}
		}
		new InstVisualization(instList);
		System.out.println("The whole instance number is " +instList.size());
	}

	public void itemsToInstances(List<item> leftList){
		this.leftInstList = new ArrayList<instance>();

		for(int i=0; i<leftList.size(); i++){
			item aItem = leftList.get(i);
			for(int j=0; j<aItem.instances.size(); j++){
				leftInstList.add(aItem.instances.get(j));
			}
		}
	}

	public void partition(){
		int numDiv = CC.numDiv;
		double[] separator= new double[numDiv];
		for(int i=1; i<=numDiv; i++)
			separator[i-1] = ((double)1/numDiv)*i;

		Collections.sort(instList, new Comparator<instance>(){
			
			public int compare(instance a, instance b){
				double ret = a.a_point.__coordinates[0] - b.a_point.__coordinates[0];
				if(ret<=0) return -1;
				else return 1;
			}
		});

		divList = new ArrayList<ArrayList<instance> >();
		for(int i=0; i<numDiv; i++)
			divList.add(new ArrayList<instance>());
		/*
		 * paritition instances to numdiv groups, based on
		 * the evenly distribution of x-axis;
		 */	
		int instCount = 0, iterCount = 1;
		for(instance inst:instList){
			//double x = inst.a_point.__coordinates[0];		
			//divList.get(findLocOfSeparator(separator,x)).add(inst);

			instCount++;
			if(instCount > iterCount*instList.size()/numDiv){
				iterCount ++;
			}
			divList.get(iterCount-1).add(inst);
		}

		/*
		 * find max point of every colum
		 */
		ArrayList<instance.point> maxList = new ArrayList<instance.point>();
		for(ArrayList<instance> col : divList){
			instance.point max = new instance.point(CC.dim);	
			max.setOneValue(0);
			for(instance inst:col){
				for(int j=0; j< CC.dim; j++){
					if(inst.a_point.__coordinates[j] > max.__coordinates[j])
						max.__coordinates[j] = inst.a_point.__coordinates[j];
				}
			}
			maxList.add(max);
		}
		/*
		 * After parition objects into several colums, we print the data based on
		 * <Area> <L>/<R> regular instance information.
		 */
		String outputFile = CC.intermediate;
		try{
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
			for(int col = 0; col<divList.size(); col++){
				int count = 0;
				int iter = 0;
				for(instance inst:divList.get(col) ){
					StringBuffer sb = new StringBuffer();
					sb.append(Integer.toString(col) + " ");
					sb.append("R" + " " + instToString(inst));
					write(bw, sb.toString());
					count++;iter++;
				}
				// Through looking for the domination relationship, it finds the
				// left bottem corner's instances.
				instance.point max = maxList.get(col);
				ArrayList<instance> coList = divList.get(col);
				for(instance inst: leftInstList){
					if(coList.contains(inst)) continue;
					if(inst.a_point.DominateAnother(max)){
						StringBuffer sb = new StringBuffer();
						sb.append(Integer.toString(col) + " ");
						sb.append("L" + " " + instToString(inst));
						write(bw, sb.toString());
						count++;
					}
				}
				System.out.println("in " + col +" this partition, instance size = "+ count);
				System.out.println("in " + col +" this partition, iter size = "+ iter);
			}
		}
		catch(IOException io){
			io.printStackTrace();
		}
	}

	public void write(BufferedWriter bw, String aString) throws IOException{
		bw.write(aString);
	}

	public String instToString(instance inst){
		int objectID = inst.objectID;
		int instID = inst.instanceID;
		String temp = Integer.toString(objectID) + " "+ Integer.toString(instID)+" ";
		temp += inst.a_point.toString();
		temp += Double.toString(inst.prob)+"\n";
		return temp;
	}

	int findLocOfSeparator(double[] separ, double x){
		int ret =0;
		for(ret=0; ret<separ.length; ret++){
			if(x<separ[ret]) break;
		}
		return ret;
	}
	
	public static void main(String[] args){
		Intermediate inter = new Intermediate(ClusterConfig.getInstance(), "../output");
		inter.getItemList(inter.getSrcInstanceFile());
		inter.readCandidates();
		inter.itemsToInstances(inter.itemList);
		inter.removeUnrelatedObjects();
		inter.itemsToInstances();
		inter.partition();

		InstNaive naive = new InstNaive(null, inter.instList);
	}
}
