package ok.gd.counters;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by olegklymchuk on 8/22/16.
 *
 */

public class ConditionalCounterMapper extends Mapper<LongWritable, BytesWritable, BytesWritable, LongWritable> {

    public static final String NUM_ATTRS = "attrs";
    public static final String ALL = "all";
    public static final String ANY = "any";
    public static final String NONE = "none";


    public static final String COUNTER_KEY = "GDC";


    protected int numAttrs;
    protected int[] allIndices;
    protected int[] anyIndices;
    protected int[] noneIndices;

    protected FilterCondition filterCondition;

    protected BytesWritable outKey = new BytesWritable(COUNTER_KEY.getBytes());
    protected LongWritable outValue = new LongWritable(0);
    protected long counter = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        numAttrs = context.getConfiguration().getInt(NUM_ATTRS, -1);
        allIndices = context.getConfiguration().getInts(ALL);
        if(allIndices != null && allIndices.length == 0) {
            allIndices = null;
        }
        anyIndices = context.getConfiguration().getInts(ANY);
        if(anyIndices != null && anyIndices.length == 0) {
            anyIndices = null;
        }
        noneIndices = context.getConfiguration().getInts(NONE);
        if(noneIndices != null && noneIndices.length == 0) {
            noneIndices = null;
        }

        filterCondition = new FilterCondition(allIndices, anyIndices, noneIndices);
    }

    @Override
    protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

        if(filterCondition.check(new Record(value.getBytes()))) {
            ++counter;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        outValue.set(counter);

        context.write(outKey, outValue);
    }

}
