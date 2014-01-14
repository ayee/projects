package com.olegklymchuk.medianfinder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MFReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    IntWritable value = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for (IntWritable iw : values)
            count += iw.get();

        value.set(count);
        context.write(key, value);
    }
}
