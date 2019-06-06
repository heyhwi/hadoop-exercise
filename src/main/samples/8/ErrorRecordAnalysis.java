package ncu.hadoop.mapreduce.practise;

// 
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

/**
 * ������ȡִ�й����еĲ���
 * 2019��5��29��
 */
public class ErrorRecordAnalysis extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		// ��ȡ��Ҫ��ȡ��jobID
		String jobID = args[0];
		Cluster cluster = new Cluster(getConf());
		Job job = cluster.getJob(JobID.forName(jobID));
		if (job == null) {
			System.err.printf("No job with ID %s found.\n", jobID);
			return -1;
		}
		if (!job.isComplete()) {
			System.err.printf("Job %s is not complete.\n", jobID);
			return -1;
		}

		Counters counters = job.getCounters();

		// ���Javaִ�й��̵ļ�����
		long longRecord = counters.findCounter(
				MyCounter.MyCounterMap.LOG_PROCESSOR_COUNTER.BAD_RECORDS_LONG)
				.getValue();
		long shortRecord = counters.findCounter(MyCounter.MyCounterMap.LOG_PROCESSOR_COUNTER.BAD_RECORDS_SHORT)
				.getValue();
		// ���MapReduceԤ����ļ�����
		long totalRecord = counters.findCounter(
				TaskCounter.MAP_INPUT_RECORDS).getValue();

		System.out.printf("Error Records : %.2f%%\n", 100.0 * (longRecord + shortRecord) / totalRecord);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ErrorRecordAnalysis(), args);
		System.exit(exitCode);
	}
}
