import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Driver class for running Text Analyzer MapReduce job.
 *
 * To run the MR job using multiple reduce jobs for better
 * utilization of your cluster, pass arguments:
 * -D com.vamsi.mapreduce.mode=cluster
 *
 * Hadoop ver: 1.1.2 MapReduce ver: MR2
 *
 * @author Vamsi Kancharla
 */

public class TextAnalyzer extends Configured implements Tool {

    private static final Logger LOGGER = Logger.getLogger(TextAnalyzer.class.getName());
    private static final String JOB_NAME = "Text Analyzer MR Job";
    private static final String CLUSTER_MODE_VALUE = "cluster";
    private static final String CLUSTER_MODE_PROPERTY = "com.vamsi.mapreduce.mode";

    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 2) {
            System.out.println("Usage: TextAnalyzer <input dir> <output dir>\n");
            return -1;
        }

        Job job = new Job(getConf());
        job.setJarByClass(TextAnalyzer.class);
        job.setJobName(JOB_NAME);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TextAnalyzerMapper.class);
        job.setMapOutputKeyClass(TextAnalyzerWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(IntSumReducer.class);

        job.setSortComparatorClass(KeyComparator.class);

        if(CLUSTER_MODE_VALUE.equalsIgnoreCase(getJobMode(job))) {
            LOGGER.debug("Cluster mode enabled");
            job.setPartitionerClass(TokenLengthPartitioner.class);
            job.setNumReduceTasks(TokenLengthPartitioner.TOTAL_PARTITIONS); //see TokenLengthPartitioner
        }

        job.setReducerClass(TextAnalyzerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(exitCode);
    }

    public static String getJobMode(Job job) {
        return job.getConfiguration().get(CLUSTER_MODE_PROPERTY);
    }
}