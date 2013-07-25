import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

    private Text keyText;
    private Text valueText;
    private String fileName;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path path = fileSplit.getPath();
        fileName = path.getName();
        keyText = new Text();
        valueText = new Text();
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException,
            InterruptedException {
        String currentLine = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(currentLine);

        while(tokenizer.hasMoreTokens()) {
            String currentWord = tokenizer.nextToken();
            keyText.set(currentWord);
            StringBuilder fileAndLineNumber = new StringBuilder(fileName);
            fileAndLineNumber.append("@").append(key.toString().toLowerCase());
            valueText.set(fileAndLineNumber.toString());
            context.write(keyText,valueText);
        }
    }
}
