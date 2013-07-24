import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProcessLogsByMonth {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage: Process Monthly Logs <input dir> <output dir>\n");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(ProcessLogsByMonth.class);
        job.setJobName("ProcessMonthlyLogs");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MonthMapper.class);
        job.setReducerClass(MonthReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(12);
        job.setPartitionerClass(MonthPartitioner.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}