import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Custom WritableComparable class. The purpose of this class is to sort the Text
 * objects (tokens) by length in ascending order.
 *
 * @author Vamsi Kancharla
 */
public class TextAnalyzerWritable implements WritableComparable<TextAnalyzerWritable> {

    private Text token;

    public TextAnalyzerWritable () {
        set(new Text());
    }

    public TextAnalyzerWritable(Text token) {
        set(token);
    }

    public TextAnalyzerWritable(String token) {
        set(new Text(token));
    }

    public void set(Text token) {
        this.token = token;
    }

    public void set(String token) {
        set(new Text(token));
    }

    public Text getToken() {
        return token;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        token.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        token.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return token.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TextAnalyzerWritable that = (TextAnalyzerWritable) o;
        if (!token.equals(that.token)) return false;
        return true;
    }

    @Override
    public int compareTo(TextAnalyzerWritable other) {
        int cmp = token.compareTo(other.getToken());
        if(cmp == 0) {
            return cmp;
        }
        else {
            int thisTokenLength = getTokenLength(token);
            int thatTokenLength = getTokenLength(other.getToken());
            if(thisTokenLength >= thatTokenLength) {
                return 1;
            }
            else {
                return -1;
            }
        }
    }

    @Override
    public String toString() {
        return token.toString();
    }

    private int getTokenLength(Text text) {
        return text.toString().length();
    }
}
