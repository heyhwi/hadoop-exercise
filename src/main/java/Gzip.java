import java.io.File;
import java.io.IOException;
import java.net.URI;

import javafx.css.StyleableStringProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;


public class Gzip {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String uri = "hdfs://hwi:9000";
        Path inputDir = new Path(uri+args[0]);
        Path outDir = new Path(args[1]);
        String codecClassname = "org.apache.hadoop.io.compress.GzipCodec";
        Class<?> codecClass = Class.forName(codecClassname);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);
        try{
            FileSystem hdfs = FileSystem.get(new URI(uri), conf);
            FileSystem local = FileSystem.getLocal(conf);
            FileStatus[] inputFiles = hdfs.listStatus(inputDir);
            FSDataOutputStream FSOut = local.create(outDir);
            CompressionOutputStream out = codec.createOutputStream(FSOut);
            for(int i=0; i<inputFiles.length; i++){
                System.out.println(inputFiles[i].getPath().getName());
                FSDataInputStream in = hdfs.open(inputFiles[i].getPath());
//                byte buffer[] = new byte[4096];
//                int bytesRead = 0;
//                while( (bytesRead = in.read(buffer)) > 0){
//                    out.write(buffer,0,bytesRead);
//                }
                IOUtils.copyBytes(in,out,4096,false);//作用与上面五行注释代码相同
                in.close();
            }
            out.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
