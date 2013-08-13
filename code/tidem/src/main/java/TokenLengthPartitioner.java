import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Custom partitioner that routes data to particular reducer based on the length of the token.
 * Use the following token distribution, with 7 total reducers. Could increase and distribute
 * to many more reducers as needed.
 *
 * Token Length     Reducer
 * 1-5              0
 * 6-8              1
 * 9-10             2
 * 11-12            3
 * 13-14            4
 * 15-16            5
 * 17+              6
 *
 * Dist of word length: http://arxiv.org/pdf/1207.2334.pdf
 *
 * The partition of token length is based on the above article (on English words distribution).
 * It would be good idea to partition word length of 1 to 3 to single partition, because
 * we might have a lot of letters such a, as, an etc.
 *
 * @author Vamsi Kancharla
 */
public class TokenLengthPartitioner <K2, V2> extends Partitioner<TextAnalyzerWritable,
        IntWritable> implements Configurable {

    static final int TOTAL_PARTITIONS = 7;

    Configuration configuration;

    HashMap<LENGTH_PARTITION, Integer> tokenLengthPartition = new HashMap<LENGTH_PARTITION, Integer>();

    @Override
    public void setConf(Configuration entries) {
        tokenLengthPartition.put(LENGTH_PARTITION.FIVE, 0); // 15,000 unique words per partition
        tokenLengthPartition.put(LENGTH_PARTITION.EIGHT, 1);
        tokenLengthPartition.put(LENGTH_PARTITION.TEN, 2);
        tokenLengthPartition.put(LENGTH_PARTITION.TWELVE, 3);
        tokenLengthPartition.put(LENGTH_PARTITION.FOURTEEN, 4);
        tokenLengthPartition.put(LENGTH_PARTITION.SIXTEEN, 5);
        tokenLengthPartition.put(LENGTH_PARTITION.DEFAULT, 6);
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public int getPartition(TextAnalyzerWritable key, IntWritable value, int numReduceTasks) {
        int tokenLength = key.getToken().toString().length();
        switch(tokenLength) {
            case 0:case 1:case 2:case 3:case 4:case 5:
                return tokenLengthPartition.get(LENGTH_PARTITION.FIVE);
            case 6:case 7:case 8:
                return tokenLengthPartition.get(LENGTH_PARTITION.EIGHT);
            case 9:case 10:
                return tokenLengthPartition.get(LENGTH_PARTITION.TEN);
            case 11:case 12:
                return tokenLengthPartition.get(LENGTH_PARTITION.TWELVE);
            case 13:case 14:
                return tokenLengthPartition.get(LENGTH_PARTITION.FOURTEEN);
            case 15:case 16:
                return tokenLengthPartition.get(LENGTH_PARTITION.SIXTEEN);
            default:
                return tokenLengthPartition.get(LENGTH_PARTITION.DEFAULT);
        }
    }

    private enum LENGTH_PARTITION {
        FIVE,
        EIGHT,
        TEN,
        TWELVE,
        FOURTEEN,
        SIXTEEN,
        DEFAULT;
    }
}
