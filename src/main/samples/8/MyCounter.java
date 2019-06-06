package ncu.hadoop.mapreduce.practise;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ����һ���ļ����淶�ĸ�ʽ��3���ֶΣ���\t����Ϊ�ָ�����������2���쳣���ݣ�һ��������ֻ��2���ֶΣ�һ����������4���ֶ�
 * ͨ����������¼��������еĴ������ݼ�¼
 */
public class MyCounter {
	// \t��
	private static String TAB_SEPARATOR = ",";

	public static class MyCounterMap extends Mapper<LongWritable, Text, Text, Text> {
		// ����ö�ٶ���
		public static enum LOG_PROCESSOR_COUNTER {
			BAD_RECORDS_LONG, BAD_RECORDS_SHORT
		};

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String arr_value[] = value.toString().split(TAB_SEPARATOR);
			if (arr_value.length > 3) {
				/* �Զ�������� */
				context.getCounter("ErrorCounter", "toolong").increment(1);
				/* ö�ټ����� */
				context.getCounter(LOG_PROCESSOR_COUNTER.BAD_RECORDS_LONG).increment(1);
			} else if (arr_value.length < 3) {
				// �Զ��������
				context.getCounter("ErrorCounter", "tooshort").increment(1);
				// ö�ټ�����
				context.getCounter(LOG_PROCESSOR_COUNTER.BAD_RECORDS_SHORT).increment(1);
			}
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Configuration conf = new Configuration();

		// �½�һ������
		Job job = new Job(conf, "MyCounter");
		// ����
		job.setJarByClass(MyCounter.class);
		// Mapper
		job.setMapperClass(MyCounterMap.class);

		// ����Ŀ¼
		FileInputFormat.addInputPath(job, new Path("testdata/input7"));
		// ���Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path("testdata/input7-out/"+System.currentTimeMillis()));
		
		// �ύ���񣬲��˳�
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}