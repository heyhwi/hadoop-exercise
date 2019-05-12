package hadoop.datetype;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

public class MyMapreMap {
	public static void main(String args[]) throws IOException{
		MapWritable a = new MapWritable();
		a.put(new IntWritable(1),new Text("Hello"));
		a.put(new IntWritable(2),new Text("World"));

		MapWritable b = new MapWritable();
		ReflectionUtils.copy(new Configuration(), a, b);
		System.out.println(b.get(new IntWritable(1)));
		System.out.println(b.get(new IntWritable(2)));
	}
}






