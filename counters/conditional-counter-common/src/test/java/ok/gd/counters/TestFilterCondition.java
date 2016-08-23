package ok.gd.counters;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by olegklymchuk on 8/22/16.
 *
 */

public class TestFilterCondition {

    @Test
    public void testFilterConditionAll() {

        FilterCondition filterCondition = new FilterCondition(new int[] {1,3,5,7,8,9,14,16}, null, null);

        Record record = new Record(1, 20);
        record.setAttribute(1);
        record.setAttribute(3);
        record.setAttribute(5);
        record.setAttribute(7);
        record.setAttribute(8);
        record.setAttribute(9);
        record.setAttribute(14);
        record.setAttribute(16);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.clearAttribute(7);

        assertFalse("Condition check was passed", filterCondition.check(record));
    }

    @Test
    public void testFilterConditionAny() {

        FilterCondition filterCondition = new FilterCondition(null, new int[] {1,3,5,7,8,9,14,16}, null);

        Record record = new Record(1, 20);
        record.setAttribute(7);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.clearAttribute(7);

        assertFalse("Condition check was passed", filterCondition.check(record));
    }

    @Test
    public void testFilterConditionNone() {

        FilterCondition filterCondition = new FilterCondition(null, null, new int[] {1,3,5,7,8,9,14,16});

        Record record = new Record(1, 20);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.setAttribute(7);

        assertFalse("Condition check was passed", filterCondition.check(record));
    }

    @Test
    public void testFilterConditionAllAny() {

        FilterCondition filterCondition = new FilterCondition(new int[] {1,3,5,7,8,9,14,16}, new int[] {10,11,12}, null);

        Record record = new Record(1, 64);
        record.setAttribute(1);
        record.setAttribute(3);
        record.setAttribute(5);
        record.setAttribute(7);
        record.setAttribute(8);
        record.setAttribute(9);
        record.setAttribute(14);
        record.setAttribute(16);

        record.setAttribute(12);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.clearAttribute(12);

        assertFalse("Condition check was passed", filterCondition.check(record));

        record.setAttribute(10);

        assertTrue("Condition check was not passed", filterCondition.check(record));
    }

    @Test
    public void testFilterConditionAllNone() {

        FilterCondition filterCondition = new FilterCondition(new int[] {1,3,5,7,8,9,14,16}, null, new int[] {30,31,32});

        Record record = new Record(1, 64);
        record.setAttribute(1);
        record.setAttribute(3);
        record.setAttribute(5);
        record.setAttribute(7);
        record.setAttribute(8);
        record.setAttribute(9);
        record.setAttribute(14);
        record.setAttribute(16);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.setAttribute(31);

        assertFalse("Condition check was passed", filterCondition.check(record));

        record.clearAttribute(31);

        assertTrue("Condition check was not passed", filterCondition.check(record));
    }

    @Test
    public void testFilterConditionAnyNone() {

        FilterCondition filterCondition = new FilterCondition(null, new int[] {10,11,12}, new int[] {30,31,32});

        Record record = new Record(1, 64);

        record.setAttribute(12);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.clearAttribute(12);

        assertFalse("Condition check was passed", filterCondition.check(record));

        record.setAttribute(12);
        record.setAttribute(31);

        assertFalse("Condition check was passed", filterCondition.check(record));

        record.clearAttribute(31);

        assertTrue("Condition check was not passed", filterCondition.check(record));
    }

    @Test
    public void testFilterConditionAllAnyNone() {

        FilterCondition filterCondition = new FilterCondition(new int[] {1,3,5,7,8,9,14,16}, new int[] {10,11,12}, new int[] {30,31,32});

        Record record = new Record(1, 64);
        record.setAttribute(1);
        record.setAttribute(3);
        record.setAttribute(5);
        record.setAttribute(7);
        record.setAttribute(8);
        record.setAttribute(9);
        record.setAttribute(14);
        record.setAttribute(16);

        record.setAttribute(12);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.setAttribute(31);

        assertFalse("Condition check was passed", filterCondition.check(record));
    }

    @Test
    public void testFilterConditionNoConditions() {

        FilterCondition filterCondition = new FilterCondition(null, null, null);

        Record record = new Record(1, 64);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.setAttribute(1);
        record.setAttribute(3);
        record.setAttribute(5);
        record.setAttribute(7);
        record.setAttribute(8);
        record.setAttribute(9);
        record.setAttribute(14);
        record.setAttribute(16);

        record.setAttribute(12);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.setAttribute(20);
        record.setAttribute(21);
        record.setAttribute(22);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.clearAttribute(20);
        record.clearAttribute(21);
        record.clearAttribute(22);

        assertTrue("Condition check was not passed", filterCondition.check(record));
    }

    @Test
    public void testFilterConditionNoConditionsInitializedWithEmptyCtor() {

        FilterCondition filterCondition = new FilterCondition();

        Record record = new Record(1, 64);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.setAttribute(1);
        record.setAttribute(3);
        record.setAttribute(5);
        record.setAttribute(7);
        record.setAttribute(8);
        record.setAttribute(9);
        record.setAttribute(14);
        record.setAttribute(16);

        record.setAttribute(12);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.setAttribute(20);
        record.setAttribute(21);
        record.setAttribute(22);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.clearAttribute(20);
        record.clearAttribute(21);
        record.clearAttribute(22);

        assertTrue("Condition check was not passed", filterCondition.check(record));
    }

    @Test
    public void testFilterConditionSettingNonCheckedAttributes() {

        FilterCondition filterCondition = new FilterCondition(new int[] {1,3,5,7,8,9,14,16}, new int[] {10,11,12}, new int[] {30,31,32});

        Record record = new Record(1, 64);
        record.setAttribute(1);
        record.setAttribute(3);
        record.setAttribute(5);
        record.setAttribute(7);
        record.setAttribute(8);
        record.setAttribute(9);
        record.setAttribute(14);
        record.setAttribute(16);

        record.setAttribute(12);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.setAttribute(20);
        record.setAttribute(21);
        record.setAttribute(22);

        assertTrue("Condition check was not passed", filterCondition.check(record));

        record.clearAttribute(20);
        record.clearAttribute(21);
        record.clearAttribute(22);

        assertTrue("Condition check was not passed", filterCondition.check(record));
    }

}
