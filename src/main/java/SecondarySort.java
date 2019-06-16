import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.imageio.IIOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.Key;
import java.util.Iterator;
import java.util.StringTokenizer;
public class SecondarySort extends Configured implements Tool{
    public static class KeyPair implements WritableComparable<KeyPair> {
        private IntWritable key1;
        private IntWritable key2;

        public KeyPair() {
            this.key1 = new IntWritable();
            this.key2 = new IntWritable();
        }

        public KeyPair(IntWritable key1, IntWritable key2) {
            this.key1 = key1;
            this.key2 = key2;
        }

        public IntWritable getKey1() {
            return key1;
        }

        public void setKey1(IntWritable key1) {
            this.key1 = key1;
        }

        public IntWritable getKey2() {
            return key2;
        }

        public void setKey2(IntWritable key2) {
            this.key2 = key2;
        }

        public void write(DataOutput dataOutput) throws IOException {
            this.key1.write(dataOutput);
            this.key2.write(dataOutput);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.key1.readFields(dataInput);
            this.key2.readFields(dataInput);
        }

        public int compareTo(KeyPair o) {   //应该是用于Map阶段sort排序
            return this.key1.compareTo(o.getKey1());
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }

    public static class SecondarySortComparator extends WritableComparator {
        protected SecondarySortComparator() {
            super(KeyPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            KeyPair keyPair1 = (KeyPair) a;
            KeyPair keyPair2 = (KeyPair) b;
            int result = keyPair1.getKey1().compareTo(keyPair2.getKey1());
            if (result == 0)
                return keyPair1.getKey2().compareTo(keyPair2.getKey2());
            else
                return result;
        }
    }

    public static class SecondaryGroupingComparator extends WritableComparator {

        protected SecondaryGroupingComparator() {
            super(KeyPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            KeyPair keyPair1 = (KeyPair) a;
            KeyPair keyPair2 = (KeyPair) b;
            return keyPair1.getKey1().compareTo(keyPair2.getKey1());
        }
    }

    public static class SecondarySortPartitioner extends Partitioner<KeyPair, IntWritable> {
        @Override
        public int getPartition(KeyPair keyPair, IntWritable value, int numPartitions) {
            return (keyPair.getKey1().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class SecondarySortMapper extends Mapper<Text, Text, KeyPair, IntWritable> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//            String[] tmp = value.toString().split("\t");
             KeyPair keyPair = new KeyPair();
             IntWritable key1 = new IntWritable(Integer.parseInt(key.toString()));
             IntWritable key2 = new IntWritable(Integer.parseInt(value.toString()));
//            key1.set(Integer.parseInt(tmp[0]));
//            key2.set(Integer.parseInt(tmp[1]));
            keyPair.setKey1(key1);
            keyPair.setKey2(key2);
            context.write(keyPair, key2);
        }
    }

    public static class SecondarySortReducer extends Reducer<KeyPair, IntWritable, Text, Text> {
        StringBuffer stringBuffer = new StringBuffer();
        Text reduceVal = new Text();
        @Override
        protected void reduce(KeyPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            stringBuffer.delete(0,stringBuffer.length());
            for (IntWritable value : values){
                stringBuffer.append(value.get() + ",");
            }
            if (stringBuffer.length() > 0) //处理末尾逗号
                stringBuffer.deleteCharAt(stringBuffer.length()-1);

            reduceVal.set(stringBuffer.toString());
            context.write(new Text(key.getKey1().toString()),reduceVal);
        }
    }


    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SecondarySort <in> [<in>...] <out>");
            System.exit(127);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SecondarySort");
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setJarByClass(SecondarySort.class);
        for (int i = 0; i < args.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(args[args.length - 1]));


        job.setMapperClass(SecondarySortMapper.class);
        job.setMapOutputKeyClass(KeyPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setNumReduceTasks(1);

        job.setReducerClass(SecondarySortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(SecondarySortComparator.class);
        job.setGroupingComparatorClass(SecondaryGroupingComparator.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        long start=System.currentTimeMillis();
        int exitCode = ToolRunner.run(new SecondarySort(),args);
        long end=System.currentTimeMillis();
        System.out.println("job successfully finished in "+ (end-start)+"ms");
        System.exit(exitCode);
    }

}
