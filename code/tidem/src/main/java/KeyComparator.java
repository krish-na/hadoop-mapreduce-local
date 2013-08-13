import exception.InvalidInputException;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

/**
 * Class used for secondary sort on TextAnalyzerWritable objects, based on their ASCII
 * Integer representation.
 *
 *
 * @author Vamsi Kancharla
 */
public  class KeyComparator extends WritableComparator {

    private static final Logger LOGGER = Logger.getLogger(KeyComparator.class.getName());

    protected KeyComparator() {
        super(TextAnalyzerWritable.class, true);
    }

    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    /**
     * Compare TextAnalyzerWritable objects. If token length match, perform comparison
     * of individual Integer representation of character of each object.
     *
     * @return Return 0 if w1 and w2 have same ASCII characters. Return 1 if first
     * has higher integer representation than second, else return -1.
     */
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
            TextAnalyzerWritable first = (TextAnalyzerWritable) w1;
            TextAnalyzerWritable second = (TextAnalyzerWritable) w2;
            int cmp = compareTokenLength(first, second);
            if(cmp == 0) {
                cmp = compareASCII(first, second);  //compare ascii char each token
            }
        return cmp;

    }

    /**
     * Compares individual characters of TextAnalyzerWritable objects for sorting purposes.
     *
     * @param first     first TextAnalyzerWritable object
     * @param second    second TextAnalyzerWritable object
     *
     * @return  Return 0 if first and second have same ASCII characters. Return 1 if first
     * has higher integer representation than second, else return -1.
     */
    private int compareASCII(TextAnalyzerWritable first, TextAnalyzerWritable second) {
        char [] firstCharArray;
        char [] secondCharArray;

        try {
            firstCharArray = getCharArray(first);
            secondCharArray = getCharArray(second);
            int charArrayLength = firstCharArray.length;
            for(int x=0; x < charArrayLength; x++) {
                int firstChar = firstCharArray[x];
                int secondChar = secondCharArray[x];
                if(firstChar == secondChar) {
                    continue;
                }
                else {
                    if(firstChar > secondChar) {
                        return 1;
                    }
                    else {
                        return -1;
                    }
                }
            }
        } catch (InvalidInputException e) {
            LOGGER.error("InvalidInputException: " + e.getStackTrace());
        }
        return 0;
    }

    /**
     * Return char array from TextAnalyzerWritable object
     *
     * @param textWritable  TextAnalyzerWritable obj
     * @return  Return char array from TextAnalyzerWritable obj.
     *
     * @throws InvalidInputException
     */
    private char[] getCharArray(TextAnalyzerWritable textWritable) throws InvalidInputException {
        if(textWritable == null || textWritable.getToken() == null
                || textWritable.getToken().toString() == null) {
            throw new InvalidInputException("TextAnalyzerWritable is null or empty");
        }
        return textWritable.getToken().toString().toCharArray();
    }

    /**
     * Leverage IntWritable compareTo() method to compare individual token lengths.
     *
     * @param t1    TextAnalyzerWritable obj
     * @param t2    TextAnalyzerWritable obj
     * @return  Return 0 if equal. Return 1 if more than, else -1.
     */
    private int compareTokenLength(TextAnalyzerWritable t1, TextAnalyzerWritable t2) {
        IntWritable first = new IntWritable(t1.getToken().toString().length());
        IntWritable second = new IntWritable(t2.getToken().toString().length());
        return first.compareTo(second);
    }
}



