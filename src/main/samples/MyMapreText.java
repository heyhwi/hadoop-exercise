package hadoop.datetype;

import org.apache.hadoop.io.Text;

public class MyMapreText {
	public static void strings(){
		String s="\u0041\u00DF\u6771\uD801\uDC00";
		System.out.println(s);
		System.out.println(s.length());
		System.out.println(s.indexOf("\u0041"));
		System.out.println(s.indexOf("\u00DF"));
		System.out.println(s.indexOf("\u6771"));
		System.out.println(s.indexOf("\uD801\uDC00")); 
		//一个候补字符，需要2个Java char类型表示
	}
	
	public static void texts(){
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		System.out.println(t.toString());
		System.out.println(t.getLength()); //返UTF-8字节数
		System.out.println(t.find("\u0041"));//1个字节  41
		System.out.println(t.find("\u00DF"));//2个字节  C391
		System.out.println(t.find("\u6771"));//3个字节  E69DB1
		System.out.println(t.find("\uD801\uDC00"));//4个字节  F0909080
		
	}
	
	public static void main(String args[]){
		strings();
		System.out.println();
		texts();	
	}
}
