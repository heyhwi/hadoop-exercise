package ncu.hadoop.mapreduce.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * MultipleOutputs实现多种文件和文件的多结构输出
 * 
 * @author weili wangwri
 * 2019年5月24日
 */
public class MultipleFileBasedColumn {
	
	public static class MultipleOutMapper extends 
	Mapper<LongWritable, Text, Text, Text>{
		
		private MultipleOutputs<Text, Text> mos;
		private Text city=new Text();
		private Text NameTell=new Text();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos=new MultipleOutputs<Text, Text>(context);
		}
		

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] values=value.toString().split(",");
			city.set(values[3]);
			NameTell.set("Name:"+values[1]+", Tel:"+values[4]);
			//输出1：传统的输出，输出全部的数据,文件名为part-
			context.write(city, NameTell);  	
			
			//输出2：为城市列生成输出文件名，文件名为具体的城市
			mos.write(city, NameTell, values[3]);	
			
			//输出3：为城市生成文件夹，可以灵活的设置输出
			mos.write(city, NameTell, values[3]+"/output");	
			
			//输出4：多种形式文件的输出
			mos.write("text",city, NameTell);	
			mos.write("seq",city, NameTell);	
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			mos.close();
		}		
	}	

	
		
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
    	Configuration conf=new Configuration();
    	Job job = new Job(conf, "word count");
    	job.setJarByClass(MultipleFileBasedColumn.class);


        FileInputFormat.setInputPaths(job,
        		new Path("testdata/input5"));
        FileOutputFormat.setOutputPath(job, 
        		new Path("testdata/input5-out/"+System.currentTimeMillis()));
        
        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, 
        		Text.class,Text.class);
        
        MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, 
        		Text.class,Text.class);        
        
//        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);	//输出Key的数据类型
        job.setOutputValueClass(Text.class);			//输出Value的数据类型
        job.setMapperClass(MultipleOutMapper.class);
        job.setNumReduceTasks(0);           
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
