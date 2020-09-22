package v2;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

public class InclusionReducer extends Reducer<Text, Text, NullWritable, BooleanWritable>{
    private int a;
    private int b;
    private int c;
    private int d;
    private boolean[][] resultArray;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        a = context.getConfiguration().getInt("a", 0);
        b = context.getConfiguration().getInt("b", 0);
        c = context.getConfiguration().getInt("c", 0);
        d = context.getConfiguration().getInt("d", 0);

        resultArray = new boolean[c + 1][d + 1];
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        BitSet bitSet = new BitSet();
        for (Text value : values) {
            clearArray();
            String[] splitted = value.toString().split(",");
            int starti = Integer.parseInt(splitted[0]);
            int stopi = Integer.parseInt(splitted[1]);
            int startj = Integer.parseInt(splitted[2]);
            int stopj = Integer.parseInt(splitted[3]);
            for (int i = starti; i <= stopi; i++) {
                Arrays.fill(resultArray[i], startj, stopj + 1, true);
            }
            boolean[][] simplified = getSimplifiedArray();
            BitSet bitSet1 = arrayToBitset(simplified);
            bitSet.or(bitSet1);
        }
        boolean writeResult = and(bitSet, (c - a + 1) * (d - b + 1));
        context.write(NullWritable.get(), new BooleanWritable(writeResult));
    }

    private void clearArray(){
        for (int i = 0; i < resultArray.length; i++) {
            Arrays.fill(resultArray[i], false);
        }

    }

    private BitSet arrayToBitset(boolean[][] array){
        BitSet bitSet = new BitSet();
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < array[i].length; j++) {
                bitSet.set(i*(array[i].length) + j, array[i][j]);
            }
        }
        return bitSet;
    }

    private boolean[][] getSimplifiedArray(){
        boolean[][] simplifiedResult = new boolean[c - a + 1][];

        for (int i = a; i <= c; i++) {
            simplifiedResult[i-a] = Arrays.copyOfRange(resultArray[i], b, d+1);
            /*for (int j = b; j <= d; j++) {
                simplifiedResult[i-a][j-b] = resultArray[i][j];
            }*/
        }

        return simplifiedResult;
    }

    private boolean isArrayEmpty(boolean[][] array){
        for (int i = 0; i < array.length; i++) {
            boolean contains = ArrayUtils.contains(array[i], true);
            if (contains) return false;
            /*for (int j = 0; j < array[i].length; j++) {
                if (array[i][j]) return false;
            }*/
        }
        return true;
    }

    private boolean and(BitSet bitSet, int len){
        for (int i = 0; i < len; i++) {
            if (bitSet.get(i) == false) return false;
        }
        return true;
    }
}
