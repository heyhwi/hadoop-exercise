package hadoop.mapreduce.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MRPreDefinedIO {
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
    	Configuration conf=new Configuration();
//    	conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",":"); 
//    	conf.set("mapred.textoutputformat.separator","--");
    	Job job = new Job(conf, "word count");
    	job.setJarByClass(MRPreDefinedIO.class);


        FileInputFormat.setInputPaths(job,
        		new Path("testdata/input3"));
        FileOutputFormat.setOutputPath(job, 
        		new Path("testdata/output0521/4"));
        
        
        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);	//���Key����������
        job.setOutputValueClass(Text.class);			//���Value����������
        job.setMapperClass(Mapper.class);				//Ԥ����
        job.setReducerClass(Reducer.class);			//Ԥ����
        
      
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
