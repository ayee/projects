package ok.gd.counters;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by olegklymchuk on 8/22/16.
 *
 */

public class ConditionalCounterReducer extends Reducer<BytesWritable, LongWritable, LongWritable, NullWritable> {

    @Override
    protected void reduce(BytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long totalCounter = 0;

        for(LongWritable value : values) {

            totalCounter += value.get();
        }

        context.write(new LongWritable(totalCounter), NullWritable.get());
    }

}
