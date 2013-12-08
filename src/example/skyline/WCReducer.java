package example.skyline;

import java.util.Iterator;
import java.util.List;

import mapreduce.*;

public class WCReducer extends Reducer {

  @Override
  public void reduce(String key, Iterator<String> values, Outputer out) {

    long sum = 0;
    
    while(values.hasNext()) {
      //sum += Long.parseLong(values.next());
	  values.next();
	  sum += 1;
    }
    
    out.collect(key, Long.toString(sum));
  }
}
