package ipAddress;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IPReducer extends MapReduceBase implements Reducer<Text, IntWritable, 
Text, IntWritable>{
	public void reduce(Text key, Iterator<IntWritable> valueList, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException{
		int wordCount = 0;
		String k_s = key.toString();
		boolean yet_to_assign = true;
		while(valueList.hasNext()){
			
			IntWritable value = valueList.next();
			if (yet_to_assign){
				k_s += "-" + value.toString();
				yet_to_assign = false;
			}
			wordCount++;
		}
		//if(k_s.contains("x"))
		output.collect(new Text(k_s),new IntWritable(wordCount));
	}
}
