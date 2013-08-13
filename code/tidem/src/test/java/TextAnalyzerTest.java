import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *  MRUnit test case of TextAnalyzer MapReduce job
 *
 *  @author Vamsi Kancharla
 */
public class TextAnalyzerTest {

    private MapDriver mapDriver;
    private ReduceDriver<TextAnalyzerWritable, IntWritable, IntWritable, Text> reduceDriver;

    @Before
    public void setUp() {
        TextAnalyzerMapper mapper = new TextAnalyzerMapper();
        TextAnalyzerReducer reducer = new TextAnalyzerReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        String inputRecord = "The quick brown fox jumped over the lazy brown dog’s back";
        mapDriver.withInput(new LongWritable(), new Text(inputRecord));
        IntWritable countOne = new IntWritable(1);
        mapDriver.withOutput(new TextAnalyzerWritable("The"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("quick"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("brown"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("fox"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("jumped"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("over"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("the"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("lazy"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("brown"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("dog’s"), countOne);
        mapDriver.withOutput(new TextAnalyzerWritable("back"), countOne);
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> singleCount = new ArrayList<IntWritable>(1);
        singleCount.add(new IntWritable(1));
        List<IntWritable> multipleCount = new ArrayList<IntWritable>();
        multipleCount.add(new IntWritable(2));

        reduceDriver.withInput(new TextAnalyzerWritable("The"), singleCount);
        reduceDriver.withInput(new TextAnalyzerWritable("fox"), singleCount);
        reduceDriver.withInput(new TextAnalyzerWritable("the"), singleCount);
        reduceDriver.withInput(new TextAnalyzerWritable("back"), singleCount);
        reduceDriver.withInput(new TextAnalyzerWritable("lazy"), singleCount);
        reduceDriver.withInput(new TextAnalyzerWritable("over"), singleCount);
        reduceDriver.withInput(new TextAnalyzerWritable("brown"), multipleCount);
        reduceDriver.withInput(new TextAnalyzerWritable("dog’s"), singleCount);
        reduceDriver.withInput(new TextAnalyzerWritable("quick"), singleCount);
        reduceDriver.withInput(new TextAnalyzerWritable("jumped"), singleCount);

        IntWritable countOfOne = new IntWritable(1);
        IntWritable countOfTwo = new IntWritable(2);
        reduceDriver.withOutput(countOfOne, new Text("The"));
        reduceDriver.withOutput(countOfOne, new Text("fox"));
        reduceDriver.withOutput(countOfOne, new Text("the"));
        reduceDriver.withOutput(countOfOne, new Text("back"));
        reduceDriver.withOutput(countOfOne, new Text("lazy"));
        reduceDriver.withOutput(countOfOne, new Text("over"));
        reduceDriver.withOutput(countOfTwo, new Text("brown"));
        reduceDriver.withOutput(countOfOne, new Text("dog’s"));
        reduceDriver.withOutput(countOfOne, new Text("quick"));
        reduceDriver.withOutput(countOfOne, new Text("jumped"));
        reduceDriver.runTest();
    }
}
