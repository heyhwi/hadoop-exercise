package hadoop.datetype;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 表示一个网络的边的数据类型（起点，终点）
 * 
 */
public class Edge implements WritableComparable<Edge> {

    private String departureNode;  //出发结点
    private String arrivalNode;    //达到结点
        
    public String getDepartureNode() { return departureNode;}
    
    @Override
    public void readFields(DataInput in) throws IOException {
        departureNode = in.readUTF();
        arrivalNode = in.readUTF();     
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(departureNode);
        out.writeUTF(arrivalNode);  
    }

    @Override
    public int compareTo(Edge o) {
     return (departureNode.compareTo(o.departureNode) != 0)
         ? departureNode.compareTo(o.departureNode)
         : arrivalNode.compareTo(o.arrivalNode);
    }
    
    
    
    
    
}