import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InvertedIndexTest {

    private MapDriver mapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private MapReduceDriver<Text, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        IndexMapper mapper = new IndexMapper();
        IndexReducer reducer = new IndexReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    /**
     * Input: 999  Use the force
     *
     * Output:
     * Use      somefile@999
     * the      somefile@999
     * force    somefile@999
     */
    @Test
    public void testMapper() throws IOException {
        String lineNum = "999";
        FileSplit fileSplit = (FileSplit) mapDriver.getContext().getInputSplit();
        Path path = fileSplit.getPath();
        String fileAndLineNum = path.getName() + "@" + lineNum;

        mapDriver.withInput(new Text(lineNum), new Text("Use the force"));
        mapDriver.withOutput(new Text("Use"), new Text(fileAndLineNum));
        mapDriver.withOutput(new Text("the"), new Text(fileAndLineNum));
        mapDriver.withOutput(new Text("force"), new Text(fileAndLineNum));
        mapDriver.runTest();
    }

    /**
     * Input: Use ["somefile@999","somefile2@123"]
     *
     * Output: Use  somefile@999,somefile2@123
     */
    @Test
    public void testReducer() throws IOException {
        String key = "Use";
        List<Text> fileAndLineNums = new ArrayList<Text>();
        fileAndLineNums.add(new Text("somefile@999"));
        fileAndLineNums.add(new Text("somefile2@123"));

        reduceDriver.withInput(new Text(key), fileAndLineNums);
        reduceDriver.withOutput(new Text(key), new Text("somefile@999,somefile2@123"));
        reduceDriver.runTest();
    }
}
