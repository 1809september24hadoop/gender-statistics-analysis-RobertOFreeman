package com.revature.testificate;

	import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import com.revature.femalegrad.FemGradMapper;
import com.revature.femalegrad.FemGradReducer;

	public class Testinator {
		public static String inputS ="\"Afghanistan\",\"AFG\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"45.02818\",\"46.33514\",\"\",\"39.55557\",\"41.65403\",\"47.07411\",\"49.00927\",\"\",\"\",";

		public static Double outputD = 49.00927;
		public static String outputS = "Afghanistan,HighSchool,";
	  /*
	   * Declare harnesses that let you test a mapper, a reducer, and
	   * a mapper and a reducer working together.
	   */
	  private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	  private ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	  private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

	  /*
	   * Set up the test. This method will be called before every test.
	   */
	  @Before
	  public void setUp() {

	    /*
	     * Set up the mapper test harness.
	     */
	    FemGradMapper mapper = new FemGradMapper();
	    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
	    mapDriver.setMapper(mapper);

	    /*
	     * Set up the reducer test harness.
	     */
	    FemGradReducer reducer = new FemGradReducer();
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
	    reduceDriver.setReducer(reducer);

	    /*
	     * Set up the mapper/reducer test harness.
	     */
	    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
	  }

	  /*
	   * Test the mapper.
	   */
	  @Test
	  public void testMapper() {

	    /*
	     * For this test, the mapper's input will be "specified string" 
	     */
	    mapDriver.withInput(new LongWritable(1), new Text(inputS));

	    /*
	     * The expected output is "specified string and 0.
	     */
	    mapDriver.withOutput(new Text(inputS), new IntWritable(0));

	    /*
	     * Run the test.
	     */
	    mapDriver.runTest();
	  }

	  /*
	   * Test the reducer.
	   */
	  @Test
	  public void testReducer() {

	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));

	    /*
	     * For this test, the reducer's input will be "Specified String".
	     */
	    reduceDriver.withInput(new Text(inputS), values);

	    /*
	     * The expected output is "Specified String and Double"
	     */
	    reduceDriver.withOutput(new Text(outputS), new DoubleWritable(outputD));

	    /*
	     * Run the test.
	     */
	    reduceDriver.runTest();
	  }

	  /*
	   * Test the mapper and reducer working together.
	   */
	  @Test
	  public void testMapReduce() {

		    /*
		     * For this test, the mapper's input will be "specified string" 
		     */
	    mapReduceDriver.withInput(new LongWritable(1), new Text(inputS));

	    /*
	     * The expected output (from the reducer) is "specified string and double". 
	     */
	    mapReduceDriver.addOutput(new Text(outputS), new DoubleWritable(outputD));

	    /*
	     * Run the test.
	     */
	    mapReduceDriver.runTest();
	  }
	}