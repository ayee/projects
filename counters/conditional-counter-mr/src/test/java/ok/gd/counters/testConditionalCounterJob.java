package ok.gd.counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by olegklymchuk on 8/22/16.
 *
 */

public class testConditionalCounterJob {

    private int numAttrs = 100;

    protected MapReduceDriver initMRDriver() {
        MapReduceDriver driver = new MapReduceDriver<LongWritable, BytesWritable, BytesWritable, LongWritable, LongWritable, NullWritable>();

        Configuration conf = driver.getConfiguration();

        conf.set(ConditionalCounterJob.INPUT_PATH, "generated-10.txt");
        conf.set(ConditionalCounterJob.OUTPUT_PATH, ".");
        conf.setInt(ConditionalCounterMapper.NUM_ATTRS, numAttrs);

        driver.setMapper(new ConditionalCounterMapper());
        driver.setReducer(new ConditionalCounterReducer());

        return driver;
    }

    @Test
    public void testCounterJobAll() throws IOException {

        MapReduceDriver driver = initMRDriver();
        driver.getConfiguration().set(ConditionalCounterMapper.ALL, "1,3,5,7,8,9,14,16");

        List<Pair<LongWritable, BytesWritable>> input = new LinkedList<Pair<LongWritable, BytesWritable>>();

        InputValueGenerator valueGenerator = new InputValueGenerator(numAttrs);
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(1), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(2), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(3), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(4), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));

        List<Pair<LongWritable, BytesWritable>> nonMatched = new LinkedList<Pair<LongWritable, BytesWritable>>();
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(5), valueGenerator.createValue(new int[] {8})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(6), valueGenerator.createValue(new int[] {9})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(7), valueGenerator.createValue(new int[] {10})));

        input.addAll(nonMatched);

        driver.addAll(input);

        List<Pair<LongWritable, NullWritable>> result = driver.run();

        assertEquals("Wrong number of outputs", 1, result.size());

        assertEquals("Wrong count", input.size() - nonMatched.size(), result.get(0).getFirst().get());
    }

    @Test
    public void testCounterJobAny() throws IOException {

        MapReduceDriver driver = initMRDriver();
        driver.getConfiguration().set(ConditionalCounterMapper.ANY, "1,3,5,7,8,9,14,16");

        List<Pair<LongWritable, BytesWritable>> input = new LinkedList<Pair<LongWritable, BytesWritable>>();

        InputValueGenerator valueGenerator = new InputValueGenerator(numAttrs);
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(1), valueGenerator.createValue(new int[] {8})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(2), valueGenerator.createValue(new int[] {8})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(3), valueGenerator.createValue(new int[] {8})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(4), valueGenerator.createValue(new int[] {8})));

        List<Pair<LongWritable, BytesWritable>> nonMatched = new LinkedList<Pair<LongWritable, BytesWritable>>();
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(5), valueGenerator.createValue(new int[] {0})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(6), valueGenerator.createValue(new int[] {2})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(7), valueGenerator.createValue(new int[] {4})));

        input.addAll(nonMatched);

        driver.addAll(input);

        List<Pair<LongWritable, NullWritable>> result = driver.run();

        assertEquals("Wrong number of outputs", 1, result.size());

        assertEquals("Wrong count", input.size() - nonMatched.size(), result.get(0).getFirst().get());
    }

    @Test
    public void testCounterJobNone() throws IOException {

        MapReduceDriver driver = initMRDriver();
        driver.getConfiguration().set(ConditionalCounterMapper.NONE, "1,3,5,7,8,9,14,16");

        List<Pair<LongWritable, BytesWritable>> input = new LinkedList<Pair<LongWritable, BytesWritable>>();

        InputValueGenerator valueGenerator = new InputValueGenerator(numAttrs);
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(1), valueGenerator.createValue(new int[] {50})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(2), valueGenerator.createValue(new int[] {50})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(3), valueGenerator.createValue(new int[] {50})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(4), valueGenerator.createValue(new int[] {50})));

        List<Pair<LongWritable, BytesWritable>> nonMatched = new LinkedList<Pair<LongWritable, BytesWritable>>();
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(5), valueGenerator.createValue(new int[] {1})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(6), valueGenerator.createValue(new int[] {3})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(7), valueGenerator.createValue(new int[] {5})));

        input.addAll(nonMatched);

        driver.addAll(input);

        List<Pair<LongWritable, NullWritable>> result = driver.run();

        assertEquals("Wrong number of outputs", 1, result.size());

        assertEquals("Wrong count", input.size() - nonMatched.size(), result.get(0).getFirst().get());
    }

    @Test
    public void testCounterJobAllAny() throws IOException {

        MapReduceDriver driver = initMRDriver();
        driver.getConfiguration().set(ConditionalCounterMapper.ALL, "1,3,5,7,8,9");
        driver.getConfiguration().set(ConditionalCounterMapper.ANY, "12,16");

        List<Pair<LongWritable, BytesWritable>> input = new LinkedList<Pair<LongWritable, BytesWritable>>();

        InputValueGenerator valueGenerator = new InputValueGenerator(numAttrs);
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(1), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(2), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(3), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(4), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));

        List<Pair<LongWritable, BytesWritable>> nonMatched = new LinkedList<Pair<LongWritable, BytesWritable>>();
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(5), valueGenerator.createValue(new int[] {1})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(6), valueGenerator.createValue(new int[] {3})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(7), valueGenerator.createValue(new int[] {5})));

        input.addAll(nonMatched);

        driver.addAll(input);

        List<Pair<LongWritable, NullWritable>> result = driver.run();

        assertEquals("Wrong number of outputs", 1, result.size());

        assertEquals("Wrong count", input.size() - nonMatched.size(), result.get(0).getFirst().get());
    }

    @Test
    public void testCounterJobAllNone() throws IOException {

        MapReduceDriver driver = initMRDriver();
        driver.getConfiguration().set(ConditionalCounterMapper.ALL, "1,3,5,7,8");
        driver.getConfiguration().set(ConditionalCounterMapper.NONE, "9,14,16");

        List<Pair<LongWritable, BytesWritable>> input = new LinkedList<Pair<LongWritable, BytesWritable>>();

        InputValueGenerator valueGenerator = new InputValueGenerator(numAttrs);
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(1), valueGenerator.createValue(new int[] {1,3,5,7,8})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(2), valueGenerator.createValue(new int[] {1,3,5,7,8})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(3), valueGenerator.createValue(new int[] {1,3,5,7,8})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(4), valueGenerator.createValue(new int[] {1,3,5,7,8})));

        List<Pair<LongWritable, BytesWritable>> nonMatched = new LinkedList<Pair<LongWritable, BytesWritable>>();
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(5), valueGenerator.createValue(new int[] {1,3,5,7,8, 9})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(6), valueGenerator.createValue(new int[] {1,3,5,7,8, 9})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(7), valueGenerator.createValue(new int[] {1,3,5,7,8, 9})));

        input.addAll(nonMatched);

        driver.addAll(input);

        List<Pair<LongWritable, NullWritable>> result = driver.run();

        assertEquals("Wrong number of outputs", 1, result.size());

        assertEquals("Wrong count", input.size() - nonMatched.size(), result.get(0).getFirst().get());
    }

    @Test
    public void testCounterJobAllAnyNone() throws IOException {

        MapReduceDriver driver = initMRDriver();
        driver.getConfiguration().set(ConditionalCounterMapper.ALL, "1,3,5,7,8,9");
        driver.getConfiguration().set(ConditionalCounterMapper.ANY, "12,16");
        driver.getConfiguration().set(ConditionalCounterMapper.NONE, "0,2,4");

        List<Pair<LongWritable, BytesWritable>> input = new LinkedList<Pair<LongWritable, BytesWritable>>();

        InputValueGenerator valueGenerator = new InputValueGenerator(numAttrs);
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(1), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(2), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(3), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(4), valueGenerator.createValue(new int[] {1,3,5,7,8,9,14,16})));

        List<Pair<LongWritable, BytesWritable>> nonMatched = new LinkedList<Pair<LongWritable, BytesWritable>>();
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(5), valueGenerator.createValue(new int[] {1})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(6), valueGenerator.createValue(new int[] {3})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(7), valueGenerator.createValue(new int[] {5})));

        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(7), valueGenerator.createValue(new int[] {0,2})));

        input.addAll(nonMatched);

        driver.addAll(input);

        List<Pair<LongWritable, NullWritable>> result = driver.run();

        assertEquals("Wrong number of outputs", 1, result.size());

        assertEquals("Wrong count", input.size() - nonMatched.size(), result.get(0).getFirst().get());
    }

    @Test
    public void testCounterJobAnyNone() throws IOException {

        MapReduceDriver driver = initMRDriver();
        driver.getConfiguration().set(ConditionalCounterMapper.ANY, "1,3,5,7,8");
        driver.getConfiguration().set(ConditionalCounterMapper.NONE, "9,14,16");

        List<Pair<LongWritable, BytesWritable>> input = new LinkedList<Pair<LongWritable, BytesWritable>>();

        InputValueGenerator valueGenerator = new InputValueGenerator(numAttrs);
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(1), valueGenerator.createValue(new int[] {3})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(2), valueGenerator.createValue(new int[] {3})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(3), valueGenerator.createValue(new int[] {3})));
        input.add(new Pair<LongWritable, BytesWritable>(new LongWritable(4), valueGenerator.createValue(new int[] {3})));

        List<Pair<LongWritable, BytesWritable>> nonMatched = new LinkedList<Pair<LongWritable, BytesWritable>>();
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(5), valueGenerator.createValue(new int[] {1,3,5,7,8,9})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(6), valueGenerator.createValue(new int[] {1,3,5,7,8,9})));
        nonMatched.add(new Pair<LongWritable, BytesWritable>(new LongWritable(7), valueGenerator.createValue(new int[] {1,3,5,7,8,9})));

        input.addAll(nonMatched);

        driver.addAll(input);

        List<Pair<LongWritable, NullWritable>> result = driver.run();

        assertEquals("Wrong number of outputs", 1, result.size());

        assertEquals("Wrong count", input.size() - nonMatched.size(), result.get(0).getFirst().get());
    }

    private static class InputValueGenerator {

        private ByteBuffer buffer;
        private Random idGenerator = new Random();
        private int numAttrs;
        private int idLength = Long.SIZE / Byte.SIZE;

        public InputValueGenerator(int numAttrs) {
            this.numAttrs = numAttrs;
            buffer = ByteBuffer.allocate(Record.size(numAttrs));
        }

        public BytesWritable createValue(int[] bitsToSet) {

            Record record = new Record(0, numAttrs);
            for(int i : bitsToSet) {
                record.setAttribute(i);
            }

            buffer.clear();

            byte[] attrs = record.getBuffer();

            byte[] byteValue = new byte[Record.size(numAttrs)];
            System.arraycopy(buffer.putLong(0, idGenerator.nextLong()).array(), 0, byteValue, 0, idLength);
            System.arraycopy(attrs, 0, byteValue, idLength, attrs.length);

            return new BytesWritable(byteValue);
        }
    }
}
