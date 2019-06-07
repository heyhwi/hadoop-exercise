import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import javax.imageio.IIOException;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class ex8_1 {
    public static boolean isHeaderPrinted = false;

    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString(); //each line
            String type = new String(); //join flag
            if(line.contains("addressId")){
                return;
            }
            StringTokenizer iter = new StringTokenizer(line);
            String mkey = new String();
            String mvalue = new String();
            int i = 0;
            while(iter.hasMoreTokens()){
                String token = iter.nextToken();
                if(token.charAt(0)>='0' && token.charAt(0)<='9'){
                    mkey = token;
                    type = i > 0 ? "1" : "2"; //1-left 2-right
                    continue;
                }
                mvalue += token + " ";
                i++;
            }
            mvalue = mvalue.trim();
            context.write(new Text(mkey), new Text(type+"-"+mvalue));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //print header
            if (false == isHeaderPrinted) {
                context.write(new Text("factoryname"), new Text("addressID" + '\t' + "addressname"));
                isHeaderPrinted = true;
            }

            int factorynum = 0, addressnum = 0;
            String[] factory = new String[10];
            String[] address = new String[10];

            Iterator iter = values.iterator();
            while (iter.hasNext()) {
                String record = iter.next().toString();
                int len = record.length();
                if (0 == len)
                    continue;

                char relationtype = record.charAt(0);
                //left
                if ('1' == relationtype) {
                    factory[factorynum] = record.substring(2);
                    factorynum++;
                }
                if ('2' == relationtype) {
                    address[addressnum] = record.substring(2);
                    addressnum++;
                }
            }
            if (0 != factorynum && 0 != addressnum) {
                for (int m = 0; m < factorynum; m++) {
                    for (int n = 0; n < addressnum; n++) {
                        System.out.println(factory[m] + '\t' + key.toString() + address[n]);
                        context.write(new Text(factory[m]), new Text(key.toString() + "\t" + address[n]));
                    }
                }
            }
//            //left join
//            if (0 != factorynum && 0 == addressnum) {
//                context.write(new Text(factory[0]), new Text("NULL"));
//            }
//            //right join
//            if (0 == factorynum && 0 != addressnum) {
//                context.write(new Text("NULL"),new Text(key.toString() + "\t" +address[0]) );
//            }
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
//        String[] ioArgs = new String[]{"MTjoin_in", "MTjoin_out"};
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length < 3){
            System.err.println("Usage: MTJoin <in1> <in2> .. <out>");
            for (int i = 0; i < otherArgs.length; ++i)
                System.out.println(otherArgs[i]);
            System.exit(2);
        }

        Job job = Job.getInstance(conf,"MTJoin");
        job.setJarByClass(ex8_1.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i)
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
