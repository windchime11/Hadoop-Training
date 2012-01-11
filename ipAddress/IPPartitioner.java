package ipAddress;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class IPPartitioner implements Partitioner<Text, IntWritable> {
	
	public void configure(JobConf job){
		
	}
	public int getPartition(Text key, IntWritable value, int numbReduceTasks){
		int month = Integer.parseInt(value.toString());
		if(month >= 0 && month < 12)
			return month;
		else
			return 0;
	}
}
