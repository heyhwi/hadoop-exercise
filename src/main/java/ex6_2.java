import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

public class ex6_2 {

    public static class Ex6Mapper extends Mapper<Object, Text, Text, LongWritable> {
        @Override
        public void map(Object key, Text value,  Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            context.write(new Text("1"), new LongWritable(Long.parseLong(value.toString())));
        }
    }

    public static class Ex6Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    }


        public static class Ex6Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            long sum = 0;
            long count = 0;
            Iterator it = values.iterator();

            for (LongWritable tempNum:values
            ) {
                long num = Long.parseLong(tempNum.toString());
                if(num > max)
                    max = num;
                if(num < min)
                    min = num;
                sum += num;
                count++;
            }
            context.write(new Text("Sum"), new LongWritable(sum));
            context.write(new Text("Max"), new LongWritable(max));
            context.write(new Text("Min"), new LongWritable(min));
            context.write(new Text("Avg"), new LongWritable(sum/count));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Max <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "ex6_2");
        job.setJarByClass(ex6_2.class);
        job.setMapperClass(Ex6Mapper.class);
        job.setCombinerClass(Ex6Reducer.class);
        job.setReducerClass(Ex6Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
