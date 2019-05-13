import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CityInfoWritable implements WritableComparable<CityInfoWritable>{
    private double longitude, latitude;
    private int population;
    private String city;



    public CityInfoWritable() {
    }

    public CityInfoWritable(double longitude, double latitude, int population, String city) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.population = population;
        this.city = city;
    }

    public int compareTo(CityInfoWritable o) {
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
        dataOutput.writeInt(population);
        dataOutput.writeChars(city);
    }

    public void readFields(DataInput dataInput) throws IOException {
        longitude = dataInput.readDouble();
        latitude = dataInput.readDouble();
        population = dataInput.readInt();
        city = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "CityInfoWritable{" +
                "longitude=" + longitude +
                ", latitude=" + latitude +
                ", population=" + population +
                ", city='" + city + '\'' +
                '}';
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        CityInfoWritable that = (CityInfoWritable) o;

        return new EqualsBuilder()
                .append(longitude, that.longitude)
                .append(latitude, that.latitude)
                .append(population, that.population)
                .append(city, that.city)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(longitude)
                .append(latitude)
                .append(population)
                .append(city)
                .toHashCode();
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public int getPopulation() {
        return population;
    }

    public void setPopulation(int population) {
        this.population = population;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}



