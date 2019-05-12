
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

public class SequenceFileWriteFile {
	private static String[] myValue = { 
		"hello world", 
		"bye world", 
		"hello hadoop", 
		"bye hadoop" 
	};

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String uri = "testdata/sqfile/sq1";
		Configuration conf = new Configuration();
		//在window中运行，关闭读取本地gzip压缩库
		conf.setBoolean("io.native.lib.available",false);
		FileSystem fs = FileSystem.getLocal(conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass(),CompressionType.NONE);
//			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass(),CompressionType.RECORD);
//			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass(),CompressionType.BLOCK);
			for (int i = 0; i < 1000; i++) {
				key.set(1000 - i);
				value.set(myValue[i % myValue.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(),key,value);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}
