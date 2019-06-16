import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
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
import sun.awt.SunGraphicsCallback;

import javax.imageio.IIOException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
    public static class SuperSortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for (Text value : values){
                String[] tmp = value.toString().split("\t");

//                context.write(value,NullWritable.get());
                context.write(key,new Text(tmp[1]));
            }
        }
    }
    public static class SuperSortPartitoner extends Partitioner<IntWritable, Text>{
        @Override
        public int getPartition(IntWritable key, Text value, int numPartitions){
            String[] tmp = value.toString().split("\t");
            int t = Integer.parseInt(tmp[0]);
            int step = 100000/numPartitions;
            for (int i=0; i<numPartitions; i++){
                if(t < step*(i+1))
                    return i;
            }

            return 0;
        }
    }

    public static void SeqGen(String[] args) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        SequenceFile.Writer writer = null;
        IntWritable key = new IntWritable();
        IntWritable value = new IntWritable();
        Path outPath = new Path(args[1]);
        Path inputPath = new Path(args[0]);

        try{
            writer = SequenceFile.createWriter(fs, conf, outPath, IntWritable.class, IntWritable.class);
            FileStatus[] inputFiles = fs.listStatus(inputPath);
            for(FileStatus fileStatus : inputFiles){
                FSDataInputStream in = fs.open(fileStatus.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String tmp = null;
                while ((tmp = br.readLine()) != null) {
                    String[] kv = tmp.split("\t");
                    key.set(new Integer(kv[0]));
                    value.set(new Integer(kv[1]));
                    writer.append(key, value);
                }
                br.close();
                in.close();
            }
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
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
        job.setNumReduceTasks(5);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setJobName("TotalSort");
        job.setPartitionerClass(SuperSortPartitoner.class);
        job.setMapperClass(SuperSortMapper.class);
        job.setReducerClass(SuperSortReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        long start=System.currentTimeMillis();
        SeqGen(args);
        long end=System.currentTimeMillis();
        System.out.println("job successfully finished in "+ (end-start)+"ms");
        return;

//        long start=System.currentTimeMillis();
//        int exitCode = ToolRunner.run(new SuperSort(),args);
//        long end=System.currentTimeMillis();
//        System.out.println("job successfully finished in "+ (end-start)+"ms");
//        System.exit(exitCode);
    }



}