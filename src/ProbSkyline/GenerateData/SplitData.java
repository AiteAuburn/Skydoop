package ProbSkyline.GenerateData;

import ProbSkyline.DataStructures.instance;
import ProbSkyline.DataStructures.item;
import ProbSkyline.DataStructures.PartitionInfo;

//import ProbSkyline.IO.TextInstanceWriter;
import ProbSkyline.IO.TextInstanceReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import java.util.Properties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class SplitData{

	public static int objectNum;
	public static int dim;
	
	public static int splitNum = 0;
	public static ArrayList< PartitionInfo > outputLists;

	public static double[] split_Value;

	private static org.apache.log4j.Logger log = Logger.getRootLogger();
	
	public static final int MaxSplitNum = 10;
	public static final boolean logEnable = false;
	//public static TextInstanceWriter[] TIWs;

	public SplitData(){

//		System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");
	}

	public static void initializeLogger(String serverId)
	{
		FileAppender fa = new FileAppender();
		fa.setName("FileLogger");
		fa.setFile(serverId + ".log");
		fa.setLayout(new PatternLayout("%d{ABSOLUTE}  %-5p [%C{1}]: %m%n"));
		fa.setThreshold(Level.INFO);
//		fa.setAppend(true);
		fa.activateOptions();
		log.addAppender(fa);
	}
	

	public static void FindleftExtremePoint(item a_item){
		int instanceNum = a_item.instances.size();
		HashSet<Integer> areaSet = new HashSet<Integer>();
		if(logEnable)
			System.out.println("access FindLeftExtremePoint");

		instance.point min,max;
		min = new instance.point(dim);
		max = new instance.point(dim);
		min.setOneValue(1.0); max.setOneValue(0.0);
		for(int i=0; i<instanceNum; i++){
			instance a_instance = a_item.instances.get(i);

			for(int j=0; j< dim; j++){
				if(a_instance.a_point.__coordinates[j] < min.__coordinates[j])
					min.__coordinates[j] = a_instance.a_point.__coordinates[j];
				if(a_instance.a_point.__coordinates[j] > max.__coordinates[j])
					max.__coordinates[j] = a_instance.a_point.__coordinates[j];
			}
		
		}
		a_item.setMin(min);
		a_item.setMax(max);

		int min_area = min.partition(split_Value,splitNum);	
		int max_area = max.partition(split_Value,splitNum);	

		PartitionInfo aPartInfo = null;
	
		for(int i=0; i<instanceNum; i++){
			instance a_instance = a_item.instances.get(i);

			int area = a_instance.a_point.partition(split_Value,splitNum);
			if(areaSet.contains(area))
				continue;
			else{
				aPartInfo = outputLists.get(area);	
				if(aPartInfo != null){
					aPartInfo.addMax(a_item.objectID,max);
					aPartInfo.addMin(a_item.objectID,min);
				}
				areaSet.add(area);
			}
		}
		

		//if(logEnable){
			//log.info("Adding size = {}", Adding.size() );
			//log.info(" a_item.id = {} ", a_item.objectID );
		//}
	}


    public static void partitionSubList(ArrayList<item> itemList,  double[] split_value){

		for(int i=0; i<itemList.size(); i++){
			item a_item = itemList.get(i);
			int length = a_item.instances.size();
			for(int j=0; j<length; j++){
				//System.out.println("instance dim = "+ a_item.instances.get(j).a_point.dimCount  );
				int ret_areaID = a_item.instances.get(j).partition(split_value, splitNum);
				//try{
				//TIWs[ret_areaID].write(a_item.instances.get(j));	
				//}catch(IOException e){e.printStackTrace();}
			}
		}

	
		//for(int i=0; i<splitNum; i++){
			//try{
			//TIWs[i].close();
			//}catch(IOException e){e.printStackTrace();}
		//}
	}

	public static void main(String args[]){

		initializeLogger("1");
		Properties prop = new Properties();
		ArrayList<item> itemList= new ArrayList<item>();

		try {
			prop.load(new FileInputStream("liang.prop"));;

		}catch (IOException ex) {
			ex.printStackTrace();
		}
		int objectNum = Integer.parseInt(prop.getProperty("objectNum"));
		int dim = Integer.parseInt(prop.getProperty("dim"));
		int split_index = 0;

		String input = prop.getProperty("srcName");
		SplitData.dim = dim;
		SplitData.objectNum = objectNum;

		if(logEnable){
			log.info("dim = "+ dim );
		}

		try {
			TextInstanceReader TIR = new TextInstanceReader(input+".txt");
			TIR.setDim(dim);

			TIR.readListItem(itemList);
			TIR.close();

			split_Value = new double[MaxSplitNum];

			FileReader	fd = new FileReader("liang.split");

			BufferedReader br = new BufferedReader(fd);
			String line = br.readLine();
			while(line != null && split_index < MaxSplitNum){

				if(Double.parseDouble(line) >=0 ){

					split_Value[split_index] = Double.parseDouble(line);
					if(logEnable){ System.out.println("split index = "+ split_index); }

					split_index ++;

				}

				line = br.readLine();
			}

			splitNum = split_index;
		   /* TIWs = new TextInstanceWriter[splitNum];*/

			//for(int i=0;i<TIWs.length;i++){

				//TIWs[i] = new TextInstanceWriter(Integer.toString(i));	
		   /* }*/

		}
		catch (FileNotFoundException foef){
			foef.printStackTrace();
		}
		catch (IOException ex){
			ex.printStackTrace();
		}

		//partitionSubList(itemList,split_Value);

		outputLists = new ArrayList<PartitionInfo>();

		for(int i=0; i<splitNum; i++){
			PartitionInfo aPartInfo = new PartitionInfo(i,dim);	
			outputLists.add(aPartInfo);
		}

		for(item i:itemList){

			FindleftExtremePoint(i);
		}

		try{

			FileOutputStream fileOut = new FileOutputStream( "MAX_MIN" );
			ObjectOutputStream outStream = new ObjectOutputStream(fileOut);
			outStream.writeObject(outputLists);
			outStream.flush();
			outStream.close();
			fileOut.close();

		}catch(IOException e){ e.printStackTrace();  }
		//catch(ClassNotFoundException cnfe){}

	}
}
