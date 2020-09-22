package v1;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.BitSet;

public class InclusionReducer extends Reducer<Text, Text, NullWritable, BooleanWritable>{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        BitSet bitSet = new BitSet();
        int len = 0;
        for (Text value : values) {
            if (len == 0) len = value.toString().length();
            BitSet valueBitSet = toBitSet(value.toString());
            bitSet.or(valueBitSet);
        }
        boolean writeResult = and(bitSet, len);
        context.write(NullWritable.get(), new BooleanWritable(writeResult));
    }

    private BitSet toBitSet(String value){
        BitSet bitSet = new BitSet();
        for (int i = 0; i < value.length(); i++) {
            bitSet.set(i, value.charAt(i) == '0' ? false : true);
        }
        return bitSet;
    }

    private boolean and(BitSet bitSet, int len){
        for (int i = 0; i < len; i++) {
            if (bitSet.get(i) == false) return false;
        }
        return true;
    }
}
