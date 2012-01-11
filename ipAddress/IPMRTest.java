package ipAddress;
import static org.junit.Assert.fail;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.TestDriver;
import org.junit.Before;
import org.junit.Test;

public class IPMRTest {
	MapDriver<LongWritable,Text,Text,IntWritable> mapDriver;
	
	@Before
	public void setup(){
		IPMapper ipm = new IPMapper();
		mapDriver = new MapDriver<LongWritable,Text,Text,IntWritable>();
		mapDriver.setMapper(ipm);
	}

	@Test
	public void testMapper(){
		mapDriver.withInput(new LongWritable(1),new Text("1.1.1.1 - - [02/Jun/2010:30:40]"));
		mapDriver.withOutput(new Text("1.1.1.1"), new IntWritable(5));
		try{mapDriver.runTest();}
		catch(Exception e){
			fail(e.getMessage());
		}
	}
}
