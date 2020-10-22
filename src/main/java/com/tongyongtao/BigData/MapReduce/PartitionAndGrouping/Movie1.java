package com.tongyongtao.BigData.MapReduce.PartitionAndGrouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: tongyongtao
 * Date: 2020-10-08
 * Time: 16:32
 */
public class Movie1 implements WritableComparable<Movie1> {
    private String movie;
    private double rate;
    private long timeStamp;
    private String uid;

    @Override
    public String toString() {
        return "Movie_TOPN{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp=" + timeStamp +
                ", uid='" + uid + '\'' +
                '}';
    }

    public Movie1(String movie, double rate, long timeStamp, String uid) {
        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
        this.uid = uid;
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

    public Movie1() {
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

    @Override
    public int compareTo(Movie1 movie1) {
        return this.movie.equals(movie1.movie) ? Double.compare(movie1.rate, this.rate) : this.movie.compareTo(movie1.movie);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {

        return   new Movie1(getMovie(),getRate(),getTimeStamp(),getUid());
    }
}
