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

import com.revature.usfemeducation.FemEduReducer;
import com.revature.usfemeducation.femeduMapper;

public class Testinatorb{
	
	public static String inputS = "\"United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"\",\"\",";
	
	public static String output1s ="Global Increase,";
	public static Double output1d = 0.8208766666666669;
	public static String output2s ="2004-2005,";
	public static Double output2d = 0.39679999999999893;
	public static String output3s ="2009-2011,";
	public static Double output3d = 2.69567;
	public static String output4s ="2007-2008,";
	public static Double output4d = 0.7822099999999992;
	public static String output5s ="2000-2001,";
	public static Double output5d = -0.39848999999999535;
	public static String output6s ="2006-2007,";
	public static Double output6d = 0.7452699999999979;
	public static String output7s ="2003-2004,";
	public static Double output7d = 0.6527200000000022;
	public static String output8s ="2011-2012,";
	public static Double output8d = 1.3011800000000022;
	public static String output9s ="2002-2003,";
	public static Double output9d = 0.9687599999999961;
	public static String output10s ="2008-2009,";
	public static Double output10d = 0.8940100000000015;
	public static String output11s ="2005-2006,";
	public static Double output11d = 1.0233300000000014;
	public static String output12s ="2001-2002,";
	public static Double output12d = 0.7890599999999992;
	
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
    femeduMapper mapper = new femeduMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
    mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
    FemEduReducer reducer = new FemEduReducer();
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
     * The expected output is a list of specified outputs configured as Strings and doubles ""
     */
    reduceDriver.addOutput(new Text(output1s), new DoubleWritable(output1d));
    reduceDriver.addOutput(new Text(output2s), new DoubleWritable(output2d));
    reduceDriver.addOutput(new Text(output3s), new DoubleWritable(output3d));
    reduceDriver.addOutput(new Text(output4s), new DoubleWritable(output4d));
    reduceDriver.addOutput(new Text(output5s), new DoubleWritable(output5d));
    reduceDriver.addOutput(new Text(output6s), new DoubleWritable(output6d));
    reduceDriver.addOutput(new Text(output7s), new DoubleWritable(output7d));
    reduceDriver.addOutput(new Text(output8s), new DoubleWritable(output8d));
    reduceDriver.addOutput(new Text(output9s), new DoubleWritable(output9d));
    reduceDriver.addOutput(new Text(output10s), new DoubleWritable(output10d));
    reduceDriver.addOutput(new Text(output11s), new DoubleWritable(output11d));
    reduceDriver.addOutput(new Text(output12s), new DoubleWritable(output12d));
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
     * The expected output (from the reducer) is "a list of outputs of specified strings and doubles". 
     */
    mapReduceDriver.addOutput(new Text(output1s), new DoubleWritable(output1d));
    mapReduceDriver.addOutput(new Text(output2s), new DoubleWritable(output2d));
    mapReduceDriver.addOutput(new Text(output3s), new DoubleWritable(output3d));
    mapReduceDriver.addOutput(new Text(output4s), new DoubleWritable(output4d));
    mapReduceDriver.addOutput(new Text(output5s), new DoubleWritable(output5d));
    mapReduceDriver.addOutput(new Text(output6s), new DoubleWritable(output6d));
    mapReduceDriver.addOutput(new Text(output7s), new DoubleWritable(output7d));
    mapReduceDriver.addOutput(new Text(output8s), new DoubleWritable(output8d));
    mapReduceDriver.addOutput(new Text(output9s), new DoubleWritable(output9d));
    mapReduceDriver.addOutput(new Text(output10s), new DoubleWritable(output10d));
    mapReduceDriver.addOutput(new Text(output11s), new DoubleWritable(output11d));
    mapReduceDriver.addOutput(new Text(output12s), new DoubleWritable(output12d));

    /*
     * Run the test.
     */
    mapReduceDriver.runTest();
  }
}

