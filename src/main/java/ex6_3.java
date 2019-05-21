import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

public class ex6_3 {

    public static class Ex6Mapper extends Mapper<Object, Text, Text, LongWritable> {
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();            context.write(new Text(filename), new LongWritable(Long.parseLong(value.toString())));
            context.write(new Text(filename), new LongWritable(Long.parseLong(value.toString())));
            System.out.println("Mapper输出<" + filename + "," + value.toString() + ">");
        }
    }

    public static class Ex6Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            long sum = 0;
            long count = 0;
            System.out.println("Reducer输入分组<" + key.toString() + ","+values.toString()+">");

            for (LongWritable tempNum:values) {
                long num = Long.parseLong(tempNum.toString());
                max = Math.max(max,num);
                min = Math.min(min,num);
                sum += num;
                count++;
                if(key.toString().equals("file1")){
                    System.err.println("Info: <"+key.toString()+","+tempNum.get()+">");
                    System.err.println("Sum = "+sum);
                }

                System.out.println("Reducer输入键值对<" + key.toString() + ","
                        + tempNum.get() + ">");
            }
            context.write(new Text(key.toString()+"-Sum"), new LongWritable(sum));
            context.write(new Text(key.toString()+"-Max"), new LongWritable(max));
            context.write(new Text(key.toString()+"-Min"), new LongWritable(min));
            context.write(new Text(key.toString()+"-Avg"), new LongWritable(sum/count));
        }

    }
    public static class Ex6Partitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text text, LongWritable longWritable, int i) {
            int location = (int) text.toString().charAt(4);
            return location%i;
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: ex6_3 <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "ex6_3");
        job.setJarByClass(ex6_3.class);
        job.setMapperClass(Ex6Mapper.class);
//        job.setCombinerClass(Ex6Combiner.class);
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

