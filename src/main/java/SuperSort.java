import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.imageio.IIOException;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
public class SuperSort extends Configured implements Tool {
    public static class SuperSortMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmp = value.toString().split("\t");
            context.write(new IntWritable(Integer.parseInt(tmp[0])),value);
        }
    }
    public static class SuperSortReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for (Text value : values){
                context.write(value,NullWritable.get());
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SuperSort <in> [<in>...] <out>");
            System.exit(127);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SuperSort");
        job.setJarByClass(SuperSort.class);

        for (int i = 0; i < args.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(args[args.length - 1]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setNumReduceTasks(4);

        job.setMapperClass(SuperSortMapper.class);
        job.setReducerClass(SuperSortReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJobName("TotalSort");

        TotalOrderPartitioner.setPartitionFile(conf, new Path( "_partitions"));
        InputSampler.RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.01, 1000, 100);
        InputSampler.writePartitionFile(job, sampler);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        long start=System.currentTimeMillis();
        int exitCode = ToolRunner.run(new SuperSort(),args);
        long end=System.currentTimeMillis();
        System.out.println("job successfully finished in "+ (end-start)+"ms");
        System.exit(exitCode);
    }


}
