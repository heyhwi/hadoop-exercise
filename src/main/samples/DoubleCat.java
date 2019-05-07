import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class DoubleCat {
	public static void main(String[] args) throws Exception {
		String uri = "hdfs://192.168.56.11:9000/user/hadoop/input2/file1";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			//ͨ�� open��������ļ�������
			in = fs.open(new Path(uri));
			System.out.println(in.getPos());
			IOUtils.copyBytes(in, System.out, 4096, false);
			System.out.println();
			System.out.println(in.getPos());
			System.out.println("***************");
			in.seek(3); // go back to pos 3 of the file
			IOUtils.copyBytes(in, System.out, 4096, false);
			
			fs.mkdirs(new Path("hdfs://192.168.56.11:9000/user/hadoop/data0417"));
		} finally {
		IOUtils.closeStream(in);
		}
	}
}
