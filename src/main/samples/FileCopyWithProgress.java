import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {
	public static void main(String[] args) throws Exception {
		String localSrc = "testdata/input/1.log";
		String dst = "hdfs://192.168.56.11:9000/user/hadoop/up0417/f1";
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable() {			
			/* (non-Javadoc)
			 * HDFS��ÿд��64K���ݰ������progress����
			 * 64K��HDFSд��datanode����С��λ
			 */
			public void progress() {
				System.out.print("*");
			}
		});
		IOUtils.copyBytes(in, out, 4096, true);
	}
}
