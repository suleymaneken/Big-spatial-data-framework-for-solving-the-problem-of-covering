package v2;

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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        a = context.getConfiguration().getInt("a", 0);
        b = context.getConfiguration().getInt("b", 0);
        c = context.getConfiguration().getInt("c", 0);
        d = context.getConfiguration().getInt("d", 0);
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

        context.write(new Text("result"), new Text(starti + "," + stopi + "," + startj + "," + stopj));
    }
}
