
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Sample average word count using Map Reduce 2.0
 */

public class AverageWordCount {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text letter = new Text();
        private IntWritable letterCount = new IntWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String currentLine = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(currentLine);

            while(tokenizer.hasMoreTokens()) {
                String currentToken = tokenizer.nextToken();
                char c = currentToken.charAt(0);
                StringBuilder b = new StringBuilder();
                b.append(c);

                letter.set(b.toString());
                letterCount.set(currentToken.length());
                context.write(letter, letterCount);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private DoubleWritable letterAverage =  new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalChars = 0.0, length = 0.0;

            for(IntWritable value: values) {
                totalChars = totalChars + value.get();
                length++;
            }

            double average = totalChars / length;
            letterAverage.set(average);
            context.write(key, letterAverage);
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.out.printf("Usage: WordCount <input dir> <output dir>\n");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(AverageWordCount.class);
        job.setJobName("AverageWordCount");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
