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
		//һ�����ַ�����Ҫ2��Java char���ͱ�ʾ
	}
	
	public static void texts(){
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		System.out.println(t.toString());
		System.out.println(t.getLength()); //��UTF-8�ֽ���
		System.out.println(t.find("\u0041"));//1���ֽ�  41
		System.out.println(t.find("\u00DF"));//2���ֽ�  C391
		System.out.println(t.find("\u6771"));//3���ֽ�  E69DB1
		System.out.println(t.find("\uD801\uDC00"));//4���ֽ�  F0909080
		
	}
	
	public static void main(String args[]){
		strings();
		System.out.println();
		texts();	
	}
}
