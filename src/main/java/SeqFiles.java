import java.io.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.util.ReflectionUtils;


public class SeqFiles {
    public static void main(String[] args) throws IOException {
//        SeqGen();
//        GetFileFromSeq("file59");
        GetDataByKey(23);
//          GetDataByKeyAndFilename(23,"file90");
    }

    static void SeqGen() throws IOException {
        String uri = "/home/cloud//work/hwiseq";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path outPath = new Path(uri);
        Path inputPath = new Path("/home/cloud/work/files_Random_small");
        SequenceFile.Writer writer = null;
        IntWritable key = new IntWritable();
        Text value = new Text();

        try {
            writer = SequenceFile.createWriter(fs, conf, outPath, IntWritable.class, Text.class);
            FileSystem local = FileSystem.getLocal(conf);
            FileStatus[] inputFiles = local.listStatus(inputPath);
            for (FileStatus fileStatus : inputFiles) {
                FSDataInputStream in = local.open(fileStatus.getPath());
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String tmp = null;
                System.out.println(fileStatus.getPath());
                while ((tmp = br.readLine()) != null) {
                    String[] kv = tmp.split("\t");
                    key.set(new Integer(kv[0]));
                    value.set(new Text(kv[1]));
                    writer.append(key, value);
                }
                br.close();
                in.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    static void GetFileFromSeq(String filename) throws IOException {
        String uri = "/home/cloud/hwiseq";
        Configuration conf = new Configuration();
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {
            SequenceFile.Reader.Option optionfile = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(conf, optionfile);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            FileSystem local = FileSystem.getLocal(conf);
            FSDataOutputStream out = local.create(new Path("/home/cloud/work/" + filename + "_read"));
            while (reader.next(key, value)) {
                String[] tmp = value.toString().split(" ");
                String currentFilename = tmp[tmp.length - 1]; //file#123
                currentFilename.replace("#", "");//file123
                if (currentFilename.equals(filename)) {
                    out.writeChars(key.toString() + '\t' + value.toString() + '\n');
//                    out.writeUTF(key.toString()+'\t'+value.toString()+'\n');
                    System.out.println(key.toString() + '\t' + value.toString());
                }

            }
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    static void GetDataByKey(Integer Key) throws IOException {
        String uri = "/home/cloud/hwiseq";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {
            SequenceFile.Reader.Option optionfile = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(conf, optionfile);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                if (key.toString().equals(Key.toString())) {
                    System.out.println(key.toString() + '\t' + value.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    static void GetDataByKeyAndFilename(Integer Key, String filename) throws IOException {
        String uri = "/home/cloud/hwiseq";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        try {
            SequenceFile.Reader.Option optionfile = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(conf, optionfile);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                String[] tmp = value.toString().split(" ");
                String currentFilename = tmp[tmp.length - 1].replace("#", "");
                if (key.toString().equals(Key.toString()) && currentFilename.equals(filename)) {
                    System.out.println(key.toString() + '\t' + value.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
