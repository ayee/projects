package ok.gd.counters;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by olegklymchuk on 8/22/16.
 *
 */

public class TestRecord {

    @Test
    public void testRecordIdInitialized() {

        Record record = new Record(1, 12);
        assertEquals("Wrong record id", 1, record.getId());
    }

    @Test
    public void testRecordAttributesInitialized() {

        int numAttrs = 12;
        Record record = new Record(1, numAttrs);

        int expectedAttrBufferLength = numAttrs / Byte.SIZE + (numAttrs % Byte.SIZE == 0 ? 0 : 1);

        assertEquals("Wrong attributes buffer length", expectedAttrBufferLength, record.getBuffer().length);
    }

    @Test
    public void testRecordSizeCalculation() {

        int idLength = Long.SIZE / Byte.SIZE;
        int numAttrs = 12;
        int attrLength = numAttrs / Byte.SIZE + (numAttrs % Byte.SIZE == 0 ? 0 : 1);
        int recordLength = idLength + attrLength;

        int actualRecordLength = Record.size(numAttrs);

        assertEquals("Wrong records size", recordLength, actualRecordLength);
    }

    @Test
    public void testRecordInitializedFromBuffer() {

        long id = new Random().nextLong();
        int idLength = Long.SIZE / Byte.SIZE;

        ByteBuffer bb = ByteBuffer.allocate(idLength);
        bb.putLong(id);

        String attrContent = UUID.randomUUID().toString();

        int numAttrs = attrContent.length() * Byte.SIZE;

        byte[] rawBuffer = new byte[Record.size(numAttrs)];
        System.arraycopy(bb.array(), 0, rawBuffer, 0, idLength);
        System.arraycopy(attrContent.getBytes(), 0, rawBuffer, idLength, attrContent.length());

        Record record = new Record(rawBuffer);
        assertEquals("Wrong Id", id, record.getId());

        byte[] recordBuffer = record.getBuffer();
        byte[] attrs = Arrays.copyOfRange(recordBuffer, idLength, recordBuffer.length);
        String actualAttrContent = new String(attrs);

        assertEquals("Wrong attributes", attrContent, actualAttrContent);
    }

    @Test
    public void testCheckAttribute() {

        Record record = new Record(1, 12);
        record.setAttribute(0);
        record.setAttribute(2);
        record.setAttribute(8);

        assertTrue("Wrong attribute state", record.checkAttribute(0));
        assertTrue("Wrong attribute state", record.checkAttribute(2));
        assertTrue("Wrong attribute state", record.checkAttribute(8));

        assertFalse("Wrong attribute state", record.checkAttribute(1));
        assertFalse("Wrong attribute state", record.checkAttribute(3));
        assertFalse("Wrong attribute state", record.checkAttribute(4));
        assertFalse("Wrong attribute state", record.checkAttribute(5));
        assertFalse("Wrong attribute state", record.checkAttribute(6));
        assertFalse("Wrong attribute state", record.checkAttribute(7));

        assertFalse("Wrong attribute state", record.checkAttribute(9));
        assertFalse("Wrong attribute state", record.checkAttribute(10));
        assertFalse("Wrong attribute state", record.checkAttribute(11));
    }

    @Test
    public void testClearAttribute() {

        Record record = new Record(1, 12);
        record.setAttribute(0);
        record.setAttribute(2);
        record.setAttribute(8);

        assertTrue("Wrong attribute state", record.checkAttribute(0));
        assertTrue("Wrong attribute state", record.checkAttribute(2));
        assertTrue("Wrong attribute state", record.checkAttribute(8));

        record.clearAttribute(0);
        record.clearAttribute(2);
        record.clearAttribute(8);

        assertFalse("Wrong attribute state", record.checkAttribute(0));
        assertFalse("Wrong attribute state", record.checkAttribute(2));
        assertFalse("Wrong attribute state", record.checkAttribute(8));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testManipulatingOutOfBoundAttrIndicesThrowsException() {

        Record record = new Record(1, 1);
        record.setAttribute(Byte.SIZE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testManipulatingOutOfBoundAttrIndicesThrowsException2() {

        Record record = new Record(1, 10);
        record.setAttribute(Byte.SIZE * 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeWithZeroAttrs() {

        new Record(1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializeWithNegativeAttrsNum() {

        new Record(1, -1);
    }

}
