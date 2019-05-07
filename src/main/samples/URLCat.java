
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class URLCat {
    static {
        //设置URL流处理工厂，在一个虚拟机中只运行运行一次，让URL识别输入流
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws Exception {
        InputStream in = null;
        try {
            //打开该URL连接，并返回输入流
            in = new URL("hdfs://hwi:9000/data/input/README.txt").openStream();

            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
