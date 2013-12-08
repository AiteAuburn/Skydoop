package example.skyline;

import mapreduce.Mapper;
import mapreduce.Outputer;
import mapreduce.ClusterConfig;
import ProbSkyline.SkyClient;

public class WCMapper extends Mapper {

	public static ClusterConfig CC;
	static{
		CC = new ClusterConfig();
	}

	@Override
	public void map(String key, String value, Outputer out) {
		String line = value;
		String partition = computeKey(line);
		out.collect("1", line);
	}

	public String computeKey(String line){
		Client
		return null;	
	}
}
