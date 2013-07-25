import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {

    private static final String COMMA = ",";
    private Text valueText = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<Text> itr = values.iterator();
        while(itr.hasNext()) {
            stringBuilder.append(itr.next());
            if(itr.hasNext()) {
                stringBuilder.append(COMMA);
            }
        }
        valueText.set(stringBuilder.toString());
        context.write(key, valueText);
    }
}
