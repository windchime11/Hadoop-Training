package InvertedIndex;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class IDMRTest {
	MapDriver<Text,Text,Text,Text> mapDriver;
	ReduceDriver<Text,Text,Text,Text> reduceDriver;
	
	@Before
	public void setup(){
		IDMapper ipm = new IDMapper();
		IDReducer idr = new IDReducer();
		mapDriver = new MapDriver<Text,Text,Text,Text>();
		mapDriver.setMapper(ipm);
		reduceDriver = new ReduceDriver<Text,Text,Text,Text>();
		reduceDriver.setReducer(idr);
	}

	@Test
	public void testMapper(){
		mapDriver.withInput(new Text("1"),new Text("cat"));
		mapDriver.withOutput(new Text("cat"), new Text("somefile@1"));
		try{mapDriver.runTest();}
		catch(Exception e){
			fail(e.getMessage());
		}
	}

	@Test
	public void testReducer(){
		List<Text> list1 = new ArrayList();
		list1.add(new Text("cat@1"));
		list1.add(new Text("dog@3"));
		list1.add(new Text("dog@1"));
		reduceDriver.withInput(new Text("cat"),list1);
		reduceDriver.withOutput(new Text("cat"), new Text("cat@1,dog@1,dog@3,"));
		try{reduceDriver.runTest();}
		catch(Exception e){
			fail(e.getMessage());
		}
	}

	
}
