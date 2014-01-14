package com.olegklymchuk.medianfinder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MFSimpleMapper extends Mapper<IntWritable, NullWritable, IntWritable, IntWritable> {

    IntWritable count = new IntWritable(1);

    @Override
    public void map(IntWritable key, NullWritable value, Context context) throws IOException, InterruptedException {

        context.write(key, count);
    }

}
