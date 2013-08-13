import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer class that sums individual key's values and emits count, key
 *
 * @author Vamsi Kancharla
 */
public class TextAnalyzerReducer extends Reducer<TextAnalyzerWritable, IntWritable, IntWritable, Text> {

    private IntWritable wordCount = new IntWritable();

    @Override
    public void reduce(TextAnalyzerWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        for(IntWritable value: values) {
            count += value.get();
        }
        wordCount.set(count);
        context.write(wordCount, key.getToken());
    }
}
