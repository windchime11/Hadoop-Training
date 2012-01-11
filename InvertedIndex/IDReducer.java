package InvertedIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class IDReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterator<Text> valueList, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException{
		String total_output = "";
		List<String> mid_list = new ArrayList<String>();
		while(valueList.hasNext()){
			Text next_v = valueList.next();
			mid_list.add(next_v.toString());
		}
		Collections.sort(mid_list);
		Iterator it =mid_list.iterator(); 
		while(it.hasNext()){
			String s_v = it.next().toString();
			total_output += s_v + ",";
		}
		
		//if(k_s.contains("x"))
		output.collect(key,new Text(total_output));
	}
}