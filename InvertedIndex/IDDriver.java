package InvertedIndex;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class IDDriver extends Configured implements Tool{

	/** 
	 * @param args
	 */

	public int run(String[] args) throws Exception {
		if(args.length != 2){
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n",
					getClass().getSimpleName());
			return -1;
		}
		JobConf jconf = new JobConf(getConf(),IDDriver.class);
		jconf.setJobName(this.getClass().getName());
		
		FileInputFormat.addInputPath(jconf,new Path(args[0]));
		FileOutputFormat.setOutputPath(jconf, new Path(args[1]));
		
		jconf.setInputFormat(KeyValueTextInputFormat.class);
		jconf.setMapperClass(IDMapper.class);
		jconf.setReducerClass(IDReducer.class);
		jconf.setMapOutputKeyClass(Text.class);
		jconf.setMapOutputValueClass(Text.class);
		jconf.setOutputKeyClass(Text.class);
		jconf.setOutputValueClass(Text.class);
		
		JobClient.runJob(jconf);
		return 0;
	}

	
	
	public static void main(String[] args) {
		try{
			int exitCode = ToolRunner.run(new IDDriver(), args);
			System.exit(exitCode);}
		catch(Exception e){
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
