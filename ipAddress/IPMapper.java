package ipAddress;

import org.apache.hadoop.mapred.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.Date;
import java.util.Calendar;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class IPMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> 
	output, Reporter reporter)
	throws IOException{
		String s = value.toString();
		if(s.length() > 15)
		{
			int first_space = s.indexOf(" ");
			String ip_s = s.substring(0, first_space);
			ip_s = ip_s.trim();
			
			int first_bracket = s.indexOf("[");
			int first_colon = s.indexOf(":");

			if(first_bracket > 0 && first_colon > 0){
			
			String date_str = s.substring(first_bracket+1, first_colon);
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy");
			Date d_fromstr;
			Calendar cl = Calendar.getInstance();
			try {
				d_fromstr = sdf.parse(date_str);
				cl.setTime(d_fromstr);
				output.collect(new Text(ip_s),new IntWritable(cl.get(Calendar.MONTH)));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
		}
	}
}
