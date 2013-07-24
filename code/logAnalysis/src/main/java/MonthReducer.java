import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MonthReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int wordCount = 0;
        while (values.iterator().hasNext()) {
            values.iterator().next();
            wordCount++;
        }
        context.write(key, new IntWritable(wordCount));
    }
}
