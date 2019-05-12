package hadoop.datetype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;


/**
 * 一个整数对的表示，可以用来表示单词出现的位置
 * 可以用第一个LongWritable代表行数
 * 第二个LongWritable代表该行的第几个单词
 */
public class NumPair implements WritableComparable<NumPair> {
	private LongWritable line;
	private LongWritable location;




	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(NumPair o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return super.equals(obj);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString();
	}

	public NumPair() {
		set(new LongWritable(0), new LongWritable(0));
	}

	public void set(LongWritable first, LongWritable second) {
		this.line = first;
		this.location = second;
	}

	public NumPair(LongWritable first, LongWritable second) {
		set(first, second);
	}

	public NumPair(int first, int second) {
		set(new LongWritable(first), new LongWritable(second));
	}

	public LongWritable getLine() {
		return line;
	}

	public LongWritable getLocation() {
		return location;
	}

//	@Override
//	public void readFields(DataInput in) throws IOException {
//		line=in.readLong();
//		line.set(in.readLong());
//		
//		line.readFields(in);
//		location.readFields(in);
//	}

//	@Override
//	public void write(DataOutput out) throws IOException {
//		line.write(out);
//		location.write(out);
//	}

	public boolean equals(NumPair o) {
		if ((this.line == o.line) && (this.location == o.location))
			return true;
		return false;
	}

//	@Override
//	public int hashCode() {
//		return line.hashCode() * 13 + location.hashCode();
//	}
//
//	@Override
//	public int compareTo(NumPair o) {
//		if ((this.line == o.line) && (this.location == o.location))
//			return 0;
//		return -1;
//	}
}
