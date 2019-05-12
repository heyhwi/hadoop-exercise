import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class MapFileWriteFile {
	private static final String[] myValue = { 
		"hello world", 
		"bye world", 
		"hello hadoop", 
		"bye hadoop" 
	};

	public static void main(String[] args) throws IOException {
		String uri = "hdfs://localhost:9000/user/wangweili/51/mapfile";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		MapFile.Writer writer = null;
		try {
			writer = new MapFile.Writer(
					conf, 
					fs, 
					uri, 
					key.getClass(), 
					value.getClass()
			);
			for (int i = 0; i < 500; i++) {
				key.set(i);
				value.set(myValue[i % myValue.length]);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}
