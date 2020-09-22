package v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InclusionFinder extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length != 6) {
            System.out.println("Usage: inclusionFinder.jar <input file> <output directory> <a> <b> <c> <d>");
            System.exit(0);
        }

        Configuration configuration = this.getConf();

        configuration.setInt("a", Integer.parseInt(args[2]));
        configuration.setInt("b", Integer.parseInt(args[3]));
        configuration.setInt("c", Integer.parseInt(args[4]));
        configuration.setInt("d", Integer.parseInt(args[5]));

        Job job = Job.getInstance(configuration, "inclusion finder v2");
        job.setJarByClass(InclusionFinder.class);
        job.setMapperClass(InclusionMapper.class);
        job.setReducerClass(InclusionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InclusionFinder(), args);
        System.exit(res);
    }
}
