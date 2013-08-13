import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class that reads one record (line), emits each word with a count of one.
 * Discard empty lines and numeric spaces.
 *
   @author Vamsi Kancharla
 */
public class TextAnalyzerMapper extends Mapper<LongWritable, Text, TextAnalyzerWritable, IntWritable> {

    private TextAnalyzerWritable textAnalyzerWritable = new TextAnalyzerWritable();
    private IntWritable countOne = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");
        for(String word: words) {
            if(!StringUtils.isNumericSpace(word)) {
                textAnalyzerWritable.set(word);
                context.write(textAnalyzerWritable, countOne);
            }
        }
    }
}
