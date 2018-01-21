package com.neu.edu.assign2part5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CustomWritable implements Writable, WritableComparable<CustomWritable>{

    private Long movieId;
    private Float rating;

    
    public CustomWritable()
    {
      
    }
   
    public CustomWritable(Long movieId, Float rating)
    {
        this.movieId = movieId;
        this.rating = rating;
    }

    public Long getMovieId() {
        return movieId;
    }

    public void setMovieId(Long movieId) {
        this.movieId = movieId;
    }

    public Float getRating() {
        return rating;
    }

    public void setRating(Float rating) {
        this.rating = rating;
    }
    
    public void write(DataOutput d) throws IOException {
        d.writeLong(movieId);
        d.writeFloat(rating);
    }

    public void readFields(DataInput di) throws IOException {
        movieId = di.readLong();
        rating = di.readFloat();
    }

    public int compareTo(CustomWritable o) {
       return -1*(rating.compareTo(o.rating));
    }

    @Override
    public String toString() {
        return movieId + ":\t" + rating ;
    }    
    
}