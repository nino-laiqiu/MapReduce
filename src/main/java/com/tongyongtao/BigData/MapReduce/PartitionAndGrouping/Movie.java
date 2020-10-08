package com.tongyongtao.BigData.MapReduce.PartitionAndGrouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-10-08
 * Time: 14:36
 */
public class Movie implements WritableComparable<Movie> {
    private String movie;
    private double rate;
    private long timeStamp;
    private String uid;

    public Movie() {
    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Movie(String movie, double rate, long timeStamp, String uid) {
        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp=" + timeStamp +
                ", uid='" + uid + '\'' +
                '}';
    }

    @Override
    public int compareTo(Movie movie) {
        //重写排序规则  用户名字相同按rate排序
        return this.uid.equals(movie.uid) ? Double.compare(movie.rate, this.rate) : this.uid.compareTo(movie.uid);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(movie);
        dataOutput.writeDouble(rate);
        dataOutput.writeLong(timeStamp);
        dataOutput.writeUTF(uid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.movie = dataInput.readUTF();
        this.rate = dataInput.readDouble();
        this.timeStamp = dataInput.readLong();
        this.uid = dataInput.readUTF();
    }

}
