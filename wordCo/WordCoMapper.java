package wordCo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCoMapper extends MapReduceBase implements Mapper<LongWritable, Text, WordCoWritable, LongWritable> {
	
	String last = null;
	
	public void map(LongWritable key, Text value, OutputCollector<WordCoWritable, LongWritable> 
	output, Reporter reporter)
	throws IOException{
		String[] s_array = value.toString().split("\\W+");
		for	(String word_in_line : s_array){
			if(word_in_line.length() > 0){
				if(last != null)
					output.collect(new WordCoWritable(last,word_in_line), new LongWritable(1));
				last = word_in_line;
			}
		}
	}
}
