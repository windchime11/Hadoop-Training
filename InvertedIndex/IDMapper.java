package InvertedIndex;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IDMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
	
	String lastWord;
	
	public void map(Text key, Text value, OutputCollector<Text, Text> 
	output, Reporter reporter)
	throws IOException{
		int lineNumber = 0;
		try {
			lineNumber = Integer.parseInt(key.toString());
		}
		catch(Exception e){
			return;
		}
		FileSplit fsplit = (FileSplit) reporter.getInputSplit();
		String file_name = fsplit.getPath().getName();
		String s = value.toString();
		for(String word : s.split("\\W+")){
			String word_trimmed = word.trim();
			String output_string = file_name + "@" + key.toString();
			if (word_trimmed.length() > 0){
				output.collect(new Text(word),new Text(output_string));
			}
		}

	}
}

