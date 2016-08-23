package ok.gd.counters;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.*;

/**
 * Created by olegklymchuk on 8/20/16.
 *
 */

public class TestConditionalCounter {

    @Test(expected = ConditionalCounterException.class)
    public void testConditionalCounterInitializationNullPath() throws ConditionalCounterException {

        new ConditionalCounter(null, 1, new FilterCondition());
    }

    @Test(expected = ConditionalCounterException.class)
    public void testConditionalCounterInitializationEmptyPath() throws ConditionalCounterException {

        new ConditionalCounter("", 1, new FilterCondition());
    }

    @Test(expected = ConditionalCounterException.class)
    public void testConditionalCounterInitializationNegativeAttrsNum() throws ConditionalCounterException {

        new ConditionalCounter("source.txt", -1, new FilterCondition());
    }

    @Test(expected = ConditionalCounterException.class)
    public void testConditionalCounterInitializationZeroAttrsNum() throws ConditionalCounterException {

        new ConditionalCounter("source.txt", 0, new FilterCondition());
    }

    @Test
    public void testCounterMatchZeroRecords() throws IOException, ConditionalCounterException {

        String path = "generated-1.txt";
        new File(path).deleteOnExit();

        int numAttrs = 100;
        int numRecords = 1;

        int[] onFlags = new int[] {1, 3, 5, 7, 8, 9, 14, 16};

        DatasetGenerator.generateRecords(path, numAttrs, numRecords, onFlags, false);

        ConditionalCounter counter = new ConditionalCounter(path, numAttrs, new FilterCondition(null, null, onFlags));
        long matchedRecords = counter.count();

        assertEquals("Wrong number of matched records", 0, matchedRecords);
    }

    @Test
    public void testCounterMatchOneRecords() throws IOException, ConditionalCounterException {

        String path = "generated-1.txt";
        new File(path).deleteOnExit();

        int numAttrs = 100;
        int numRecords = 1;

        int[] onFlags = new int[] {1, 3, 5, 7, 8, 9, 14, 16};

        DatasetGenerator.generateRecords(path, numAttrs, numRecords, onFlags, false);

        ConditionalCounter counter = new ConditionalCounter(path, numAttrs, new FilterCondition(onFlags, null, null));
        long matchedRecords = counter.count();

        assertEquals("Wrong number of matched records", numRecords, matchedRecords);
    }

    @Test
    public void testCounterMatchFourRecords() throws IOException, ConditionalCounterException {

        String path = "generated-4.txt";
        new File(path).deleteOnExit();

        int numAttrs = 100;
        int numRecords = 4;

        int[] onFlags = new int[] {1, 3, 5, 7, 8, 9, 14, 16};

        DatasetGenerator.generateRecords(path, numAttrs, numRecords, onFlags, false);

        ConditionalCounter counter = new ConditionalCounter(path, numAttrs, new FilterCondition(onFlags, null, null));
        long matchedRecords = counter.count();

        assertEquals("Wrong number of matched records", numRecords, matchedRecords);
    }

    @Test
    public void testCounterMatchTenRecords() throws IOException, ConditionalCounterException {

        String path = "generated-10.txt";
        new File(path).deleteOnExit();

        int numAttrs = 100;
        int numRecords = 10;

        int[] onFlags = new int[]{1, 3, 5, 7, 8, 9, 14, 16};

        DatasetGenerator.generateRecords(path, numAttrs, numRecords, onFlags, false);

        ConditionalCounter counter = new ConditionalCounter(path, numAttrs, new FilterCondition(onFlags, null, null));
        long matchedRecords = counter.count();

        assertEquals("Wrong number of matched records", numRecords, matchedRecords);
    }

    @Test
    public void testCounterNonMatchedRecordsNotCounted() throws IOException, ConditionalCounterException {

        String path = "generated-4.txt";
        new File(path).deleteOnExit();

        int numAttrs = 100;
        int numRecords = 4;

        int[] onFlags = new int[] {1, 3, 5, 7};
        DatasetGenerator.generateRecords(path, numAttrs, numRecords, onFlags, false);

        int[] nonMatchingFlags = new int[] {0, 2, 4, 8};
        DatasetGenerator.generateRecords(path, numAttrs, numRecords + 2, nonMatchingFlags, true);

        ConditionalCounter counter = new ConditionalCounter(path, numAttrs, new FilterCondition(onFlags, null, null));
        long matchedRecords = counter.count();

        assertEquals("Wrong number of matched records", numRecords, matchedRecords);
    }

}
