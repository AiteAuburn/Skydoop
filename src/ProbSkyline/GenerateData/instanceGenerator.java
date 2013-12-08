package org.liang.GenerateData;

import org.uncommons.maths.random.GaussianGenerator;
import org.uncommons.maths.random.MersenneTwisterRNG;
import java.util.Random;
import org.uncommons.maths.random.ContinuousUniformGenerator;
import java.lang.Math;
import java.io.BufferedWriter;
import java.io.File;
import java.util.Properties;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.liang.DataStructures.instance;
import org.liang.IO.TextInstanceWriter;

public class instanceGenerator{
	private Random rng;
	private ContinuousUniformGenerator CUG;
	private GaussianGenerator GG;
	private static int objectNum;
	private static int dim;

	public instanceGenerator(double begin, double end){
		this.rng = new MersenneTwisterRNG();
		CUG = new ContinuousUniformGenerator(begin, end, rng);
		GG = new GaussianGenerator(0.05, 0.0125, rng);
	}

	public double IndependantRange(){
		return CUG.nextValue(); 
	}

	public double generateEdgeWidth(){
	
		return this.GG.nextValue();	
	}

	public instance.point gen_a_instance(instance.point a_center, double edgeSize){
	
		double halfEdge = edgeSize / 2;

		instance.point pt= new instance.point(dim);
		ContinuousUniformGenerator instDimGen = new ContinuousUniformGenerator(-1*halfEdge, halfEdge, rng);

		for(int i=0; i <dim; i++){

			double a_value;
			while(true){
				a_value = instDimGen.nextValue();
				if( a_center.__coordinates[i]+a_value > 0 && a_center.__coordinates[i]+a_value<1) break;
			}

		pt.setValue(a_center, i, a_value);
		}
		return pt;
	}

	public double[] randomFixedSum(int a_num){
	
		ContinuousUniformGenerator	rfs = new ContinuousUniformGenerator(0.0, 1.0, this.rng);

		double [] ret = new double[a_num];
		ret[0] = 0.0;
		for(int i=1; i<a_num; i++){
			ret[i] = rfs.nextValue();
		}
		Arrays.sort(ret);
		for(int i=0; i<a_num-1; i++){
			ret[i] = ret[i+1] - ret[i];
		}
		ret[a_num-1] = 1-ret[a_num - 1];
		return ret;
	}

	public static void main(String args[]){
	
	instanceGenerator IG = new instanceGenerator(0.1,0.9);

	Properties prop = new Properties();
	try {
		prop.load(new FileInputStream("liang.prop"));;

    	}catch (IOException ex) {
    		ex.printStackTrace();
		}
	objectNum = Integer.parseInt(prop.getProperty("objectNum"));
	dim = Integer.parseInt(prop.getProperty("dim"));
	String fileName = prop.getProperty("srcName");

	File file;
	TextInstanceWriter TIW; 
	int instanceID = 0;
	try{
		BufferedReader br;
		FileReader	fd = new FileReader(fileName);
		br = new BufferedReader(fd);

		file = new File(fileName+ ".txt");
		if (!file.exists()) {
			file.createNewFile();
		}

		TIW = new TextInstanceWriter(fileName);
		/*
		 * instance number follows the uniform distribution from 1 to 50.
		 */
		ContinuousUniformGenerator CUFG	= new ContinuousUniformGenerator(1, 20, IG.rng);
		for(int i=0; i<objectNum; i++){
			
			String line = br.readLine();
			if(line!=null){
				String[] parts = line.split(" ");
				double[] center = new double[dim];
				for(int j=0; j<dim; j++){
					center[j] = Double.parseDouble(parts[j]);
				}
				instance.point a_center = new instance.point(dim);
				a_center.setPoint(center);
				int instanceNum = (int)Math.round( CUFG.nextValue() );

				double edgeWidth = IG.generateEdgeWidth();
				double prob[] = IG.randomFixedSum(instanceNum);

				for(int j=0; j<instanceNum; j++){

					instance ret_instance = new instance(IG.gen_a_instance(a_center, edgeWidth));

					ret_instance.setInstanceID(instanceID);
					ret_instance.setObjectID(i);
					ret_instance.setProb(prob[j]);
					//System.out.println(ret_instance.toString());
					TIW.write(ret_instance);
					instanceID += 1;
				}
			}

		}
		TIW.close();
		br.close();	
	   }catch (FileNotFoundException foef){
		foef.printStackTrace();
	   }catch (IOException e) {
		e.printStackTrace();
	   }

	}
}
