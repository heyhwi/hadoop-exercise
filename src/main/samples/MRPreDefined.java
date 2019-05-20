package hadoop.mapreduce.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;


public class MRPreDefined {
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
    	Configuration conf=new Configuration();
    	Job job = new Job(conf, "word count");
    	job.setJarByClass(MRPreDefined.class);


        FileInputFormat.setInputPaths(job, new Path("testdata/input3"));
        FileOutputFormat.setOutputPath(job, new Path("testdata/output0514/4"));

        //test1 ֱ�����
//        job.setOutputKeyClass(LongWritable.class);	//���Key����������
//        job.setOutputValueClass(Text.class);			//���Value����������
//        job.setMapperClass(Mapper.class);				//Ԥ����
//        job.setReducerClass(Reducer.class);			//Ԥ����


        //test2 ��ת���
//        job.setOutputKeyClass(Text.class);             //���Key����������
//        job.setOutputValueClass(LongWritable.class);   //���Value����������
//        job.setMapperClass(InverseMapper.class);    //Ԥ����
//        job.setReducerClass(Reducer.class);    //Ԥ����
        
        
        //test3 ������
        job.setOutputKeyClass(Text.class);             //���Key����������
        job.setOutputValueClass(IntWritable.class);   //���Value����������
        job.setMapperClass(TokenCounterMapper.class);    //Ԥ����
        job.setReducerClass(IntSumReducer.class);    //Ԥ����
        

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}





