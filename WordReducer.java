import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordReducer extends MapReduceBase implements Reducer<Text, IntWritable, 
Text, IntWritable>{
	public void reduce(Text key, Iterator<IntWritable> valueList, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException{
		int wordCount = 0;
		String k_s = key.toString();
		while(valueList.hasNext()){
			IntWritable value = valueList.next();
			wordCount += value.get();
		}
		if(k_s.contains("x"))
			output.collect(key,new IntWritable(wordCount));
	}
}
