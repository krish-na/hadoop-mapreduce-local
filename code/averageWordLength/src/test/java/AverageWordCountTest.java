import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AverageWordCountTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;

    @Before
    public void setUp() {
        AverageWordCountMapper mapper = new AverageWordCountMapper();
        AverageWordCountReducer reducer = new AverageWordCountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() {
        mapDriver.withInput(new LongWritable(), new Text("Use the force User"));
        mapDriver.withOutput(new Text("U"), new IntWritable(3));
        mapDriver.withOutput(new Text("t"), new IntWritable(3));
        mapDriver.withOutput(new Text("f"), new IntWritable(5));
        mapDriver.withOutput(new Text("U"), new IntWritable(4));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(3));
        values.add(new IntWritable(4));
        reduceDriver.withInput(new Text("U"), values);
        reduceDriver.withOutput(new Text("U"), new DoubleWritable(3.5));
        reduceDriver.runTest();
    }
}
