import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MonthMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields = value.toString().split(" ");

        if (fields.length > 3) {
            String ip = fields[0];

            String[] dtFields = fields[3].split("/");
            if (dtFields.length > 1) {
                String theMonth = dtFields[1];
                context.write(new Text(ip), new Text(theMonth));
            }
        }
    }

}
