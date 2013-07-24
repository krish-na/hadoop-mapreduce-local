import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImageCounter extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: ImageFormatsCounter <input dir> <output dir>\n");
            return -1;
        }

        Job job = new Job(getConf());
        job.setJarByClass(ImageCounter.class);
        job.setJobName("ImageFormatsCounter");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ImageCounterMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);

        boolean success = job.waitForCompletion(true);
        if(success) {
            long jpg = job.getCounters().findCounter("ImageFormats", "jpg").getValue();
            long gif = job.getCounters().findCounter("ImageFormats", "gif").getValue();
            long vam = job.getCounters().findCounter("ImageFormats", "vam").getValue();
            System.out.println("jpg: " + jpg);
            System.out.println("GIF: " + gif);
            System.out.println("VAM: " + vam);
            return 0;
        } else
            return 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ImageCounter(), args);
        System.exit(exitCode);
    }
}
