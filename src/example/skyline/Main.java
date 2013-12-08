package example.skyline;

import mapreduce.*;

public class Main {

  public static void main(String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: skyline <input_path> <output_path> <jar_path>");
      return ;
    }
    
    JobConf jconf = new JobConf();
    jconf.setJobName("ProbSkyline");
    
    jconf.setInputPath(args[0]);
    jconf.setOutputPath(args[1]);
    jconf.setJarFilePath(args[2]);
    
    jconf.setBlockSize(1000000);
    
    jconf.setMapperClassName("example.skyline.WCMapper");
    jconf.setReducerClassName("example.skyline.WCReducer");
    
    jconf.setInputFormatClassName("example.skyline.WCInputFormat");
    jconf.setOutputFormatClassName("example.skyline.WCOutputFormat");
    jconf.setPartitionerClassName("example.skyline.WCPartitioner");
    
    jconf.setReducerNum(4);
    
    Job job = new Job(jconf);
    job.run();
  }

}
