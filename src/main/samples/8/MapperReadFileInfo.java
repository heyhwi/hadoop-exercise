package hadoop.mapreduce.io;

import java.io.IOException;

import javax.print.attribute.standard.NumberOfDocuments;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapperReadFileInfo {

	public static class ReadFileInfoMapper 
	extends Mapper<Object, Text, NullWritable, Text> {
		
		public static int NumberOfMapper=0;		
		
		@Override
		protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			NumberOfMapper++;
			super.setup(context);
			
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//��ȡ��ȡ���ļ���Ϣ
			//�ļ�·��+
			FileSplit file = (FileSplit) context.getInputSplit();			
			String fileName=file.getPath().getName();  //��ȡ���ļ���
			long start=file.getStart();					//��Ӧ���ݷ�Ƭ�����ͳ���
			long length=file.getLength();
			String fileinfo="Split# "+NumberOfMapper+" from "+fileName+"("+start+":"+ length+")--"+value;
			context.write(NullWritable.get(),new Text(fileinfo));
		}
	}


	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//���������޸����ݷ�Ƭ�Ĵ�С������Ϊ10���ֽڣ�����ʾ
		conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 10L);
		String[] otherArgs = { "testdata/input4", "testdata/output0521/2" };
		Job job = new Job(conf, "fileinfo");
		job.setJarByClass(MapperReadFileInfo.class);
		job.setMapperClass(ReadFileInfoMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);


		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
