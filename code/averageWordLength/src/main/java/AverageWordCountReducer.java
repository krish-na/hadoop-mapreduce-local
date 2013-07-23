import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AverageWordCountReducer extends Reducer <Text, IntWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        double totalChars = 0.0, length = 0.0;

        for(IntWritable value: values) {
            totalChars = totalChars + value.get();
            length++;
        }

        double average = totalChars / length;
        DoubleWritable letterAverage =  new DoubleWritable();
        letterAverage.set(average);
        context.write(key, letterAverage);
    }
}
