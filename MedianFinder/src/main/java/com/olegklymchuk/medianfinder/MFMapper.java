package com.olegklymchuk.medianfinder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MFMapper extends Mapper<IntWritable, NullWritable, IntWritable, IntWritable> {

    TreeMap<Integer, Integer> tm = new TreeMap<Integer, Integer>();
    IntWritable out_key = new IntWritable();
    IntWritable out_value = new IntWritable();

    @Override
    public void map(IntWritable key, NullWritable value, Context context) throws IOException, InterruptedException {

        Integer count = tm.get(key.get());
        if (count == null)
            tm.put(key.get(), 1);
        else
            tm.put(key.get(), ++count);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<Integer, Integer> entry : tm.entrySet()) {

            out_key.set(entry.getKey());
            out_value.set(entry.getValue());
            context.write(out_key, out_value);
        }
    }

}
