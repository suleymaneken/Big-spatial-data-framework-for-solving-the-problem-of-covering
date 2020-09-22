package v1;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class InclusionMapper extends Mapper<Object, Text, Text, Text> {

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
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] splittedLine = line.split(",");
        int w, x, y, z;
        try {
            w = Integer.parseInt(splittedLine[0]);
            x = Integer.parseInt(splittedLine[1]);
            y = Integer.parseInt(splittedLine[2]);
            z = Integer.parseInt(splittedLine[3]);
        } catch (NumberFormatException ex) {
            return;
        }

        int starti = w < a ? a : w;
        int stopi = y < c ? y : c;
        int startj = x < b ? b : x;
        int stopj = z < d ? z : d;

        if (starti > stopi || startj > stopj) return;

        clearArray();

        for (int i = starti; i <= stopi; i++) {
            /*for (int j = startj; j <= stopj; j++) {
                resultArray[i][j] = true;
            }*/
            Arrays.fill(resultArray[i], startj, stopj + 1, true);
        }

        boolean[][] simpleResultArray = getSimplifiedArray();

        if (!isArrayEmpty(simpleResultArray)){
            String output = getArrayText(simpleResultArray);
            context.write(new Text("result"), new Text(output));
        }
    }

    private void clearArray(){
        for (int i = 0; i < resultArray.length; i++) {
            Arrays.fill(resultArray[i], false);
        }

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

    private String getArrayText(boolean[][] array){
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            StringBuilder row = new StringBuilder();
            for (int j = 0; j < array[i].length; j++) {
                if (array[i][j]) row.append("1");
                else row.append("0");
            }
            output.append(row);
        }

        return output.toString();
    }
}
