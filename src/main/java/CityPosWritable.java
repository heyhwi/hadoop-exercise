import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

public class CityPosWritable implements WritableComparable<CityPosWritable>{
    private double longitude, latitude;


    public CityPosWritable() {
    }

    public CityPosWritable(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public int compareTo(CityPosWritable o) {
        if (longitude == o.longitude){
            if (latitude == o.latitude)
                return 0;
            else
                return latitude > o.latitude ? 1: -1;
        }
        else
            return longitude > o.longitude ? 1: -1;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(longitude);
        dataOutput.writeDouble(latitude);
    }

    public void readFields(DataInput dataInput) throws IOException {
        longitude = dataInput.readDouble();
        latitude = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "CityInfoWritable{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        CityPosWritable that = (CityPosWritable) o;

        return new EqualsBuilder()
                .append(longitude, that.longitude)
                .append(latitude, that.latitude)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(longitude)
                .append(latitude)
                .toHashCode();
    }
}



