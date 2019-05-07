import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatus {
	public static void main(String[] args) throws Exception {
		String uri = args[0];
		Configuration conf = new Configuration();
		//�����ļ�ϵͳʵ��fs
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path[] paths = new Path[args.length];
		for (int i = 0; i < paths.length; i++) {
			paths[i] = new Path(args[i]);
		}
		
		FileStatus[] status = fs.listStatus(paths);
		Path[] listedPaths = FileUtil.stat2Paths(status);   //convert an array of FileStatus to an array of Path
		for (int i=0;i<status.length;i++){
			System.out.println(listedPaths[i]);
			System.out.print("\t"+status[i].getOwner()+"\t"+status[i].getPermission().toString()+"\n");
		}
//		for (Path p : listedPaths) {
//			System.out.println(p);
//		}
	}
}
