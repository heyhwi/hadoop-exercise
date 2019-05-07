import java.io.File;
import java.io.FileOutputStream;

// cc StreamCompressor A program to compress data read from standard input and write it to standard output
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

// vv StreamCompressor
/**
 * ���У�
 * echo "hello hadoop;hello java" | hadoop hadoop.io.StreamCompressorFile abc.gz 
 * ʵ���ı�д��ѹ���ļ�abc.gz
 * 
 */
public class StreamCompressorFile {

	public static void main(String[] args) throws Exception {
		//ͨ����������һ��codec��ʵ��
		String compressedFileUri = args[0];
		String codecClassname = "org.apache.hadoop.io.compress.GzipCodec";
		Class<?> codecClass = Class.forName(codecClassname);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

		CompressionOutputStream out = codec.createOutputStream(new FileOutputStream(new File(compressedFileUri)));
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.finish();
	}
}

