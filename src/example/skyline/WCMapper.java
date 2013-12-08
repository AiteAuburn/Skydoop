package example.skyline;

import mapreduce.Mapper;
import mapreduce.Outputer;
import mapreduce.ClusterConfig;
import ProbSkyline.SkyClient;

public class WCMapper extends Mapper {

	@Override
	public void map(String key, String value, Outputer out) {
		String line = value;
		String partition = computeKey(line);
		out.collect(partition, line);
	}

	public String computeKey(String line){
		SkyClient client = new SkyClient(ClusterConfig.getInstance(), line);
		return Integer.toString(client.getPartition());	
	}
}
