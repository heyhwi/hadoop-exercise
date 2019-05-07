// cc StreamCompressor A program to compress data read from standard input and write it to standard output
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

// vv StreamCompressor
// ��������
//echo "hello Java Compressor" | hadoop hadoop.io.StreamCompressor ʵ�ֶ��ı���ѹ��
//echo "hello Java Compressor" | hadoop hadoop.io.StreamCompressor |gunzip ʵ�ֶ��ı���ѹ������ѹ

public class StreamCompressor {

	public static void main(String[] args) throws Exception {
		//ͨ����������һ��codec��ʵ��
		//String codecClassname = args[0];
		String codecClassname = "org.apache.hadoop.io.compress.GzipCodec";
		Class<?> codecClass = Class.forName(codecClassname);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

		CompressionOutputStream out = codec.createOutputStream(System.out);
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.finish();
	}
}

