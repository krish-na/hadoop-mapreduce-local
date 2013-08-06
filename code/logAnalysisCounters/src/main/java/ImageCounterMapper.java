import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

/**
 * Ex input line:
 * 10.82.30.199 - - [28/Jul/2009:08:58:48 -0700] "GET /assets/search-button.gif HTTP/1.1" 200 168
 *
 */
public class ImageCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Logger LOGGER = Logger.getLogger(ImageCounterMapper.class.getName());
    HashMap<String, Object> imageFormats;
    Object acceptedImg;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        imageFormats = new HashMap<String, Object>();
        acceptedImg = new Object();
        imageFormats.put("jpg", acceptedImg);
        imageFormats.put("png", acceptedImg);
        imageFormats.put("gif", acceptedImg);
        imageFormats.put("tiff", acceptedImg);
        imageFormats.put("jpeg", acceptedImg);
        imageFormats.put("ico", acceptedImg);
        imageFormats.put("vam", acceptedImg);  //just for fun

        Configuration conf = context.getConfiguration();
        if ("DEBUG".equals(conf.get("com.vamsi.log4j.level"))) {
            LOGGER.setLevel(Level.DEBUG);
            LOGGER.debug("->Log Level set to DEBUG <-");
        } else if("INFO".equals(conf.get("com.vamsi.log4j.level"))) {
            LOGGER.setLevel(Level.INFO);
            LOGGER.info("->Log Level set to INFO <-");
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String values [] = line.split(" ");
        String imageVal = values[6];  //  /assets/search-button.gif
        String imgExt  = getImgExtension(imageVal);

        if(imgExt != null && imgExt.length() > 2 && isAcceptedImgFormat(imgExt)) {
            context.getCounter("ImageFormats", imgExt).increment(1);
        }
    }

    public String getImgExtension(String val) {
        if(val == null && val.length() < 3) {
            return null;
        }
        StringBuilder imgExtension = new StringBuilder();
        for(int x = val.length() - 1 ; x >= 0; x--) {
            char currentChar = val.charAt(x);
            if(currentChar == '.') {
                break;
            }
            else {
                imgExtension.append(currentChar);
            }
        }
        return imgExtension.reverse().toString();
    }

    public boolean isAcceptedImgFormat(String imgExtension) {
         if(imageFormats.get(imgExtension) == null) {
             return false;
         }
        else {
             return true;
         }
    }
}
