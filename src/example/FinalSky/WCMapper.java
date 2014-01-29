package example.FinalSky;

import mapreduce.Mapper;
import mapreduce.Outputer;
import mapreduce.ClusterConfig;

public class WCMapper extends Mapper {

	@Override
	public void map(String key, String value, Outputer out) {
		String [] line = value.split(" "); 
		out.collect(line[0], value);
	}

