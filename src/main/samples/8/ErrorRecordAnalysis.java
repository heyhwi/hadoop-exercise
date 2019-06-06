package ncu.hadoop.mapreduce.practise;

// 
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

/**
 * 用来获取执行过程中的参数
 * 2019年5月29日
 */
public class ErrorRecordAnalysis extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		// 获取需要读取的jobID
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

		// 获得Java执行过程的计数器
		long longRecord = counters.findCounter(
				MyCounter.MyCounterMap.LOG_PROCESSOR_COUNTER.BAD_RECORDS_LONG)
				.getValue();
		long shortRecord = counters.findCounter(MyCounter.MyCounterMap.LOG_PROCESSOR_COUNTER.BAD_RECORDS_SHORT)
				.getValue();
		// 获得MapReduce预定义的计数器
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
