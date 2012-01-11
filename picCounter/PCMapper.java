package picCounter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PCMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> 
	output, Reporter reporter)
	throws IOException{

		String log_r = value.toString();
		
		if(log_r.contains("img")){
			if(log_r.contains("gif"))
				reporter.incrCounter("ImageCounter","gifCounter",1);
			else{
				if(log_r.contains("jpg"))
					reporter.incrCounter("ImageCounter","jpegCounter",1);
				else
					reporter.incrCounter("ImageCounter","otherCounter",1);
			}
		}
		
	}
}

