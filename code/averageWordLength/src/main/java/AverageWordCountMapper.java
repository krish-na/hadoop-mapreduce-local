import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class AverageWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String currentLine = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(currentLine);

        while(tokenizer.hasMoreTokens()) {
            String currentToken = tokenizer.nextToken();
            char c = currentToken.charAt(0);
            StringBuilder b = new StringBuilder();
            b.append(c);

            Text letter = new Text();
            IntWritable letterCount = new IntWritable();

            letter.set(b.toString());
            letterCount.set(currentToken.length());
            context.write(letter, letterCount);
        }
    }
}
