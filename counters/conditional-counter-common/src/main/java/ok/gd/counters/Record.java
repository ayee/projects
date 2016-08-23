package ok.gd.counters;

/**
 * Created by olegklymchuk on 8/22/16.
 *
 */

public class Record {

    protected long id;
    protected byte[] buffer;
    protected int offset = 0;

    public Record(long id, int numAttrs) {

        if(numAttrs < 1) {
            throw new IllegalArgumentException("Number of attributes should be at least 1");
        }

        this.id = id;
        buffer = new byte[numAttrs / Byte.SIZE + (numAttrs % Byte.SIZE == 0 ? 0 : 1)];
    }

    public Record(byte[] raw) {

        id = readId(raw);

        buffer = raw;
        offset = Long.SIZE / Byte.SIZE;
    }

    public static int size(int numAttrs) {
        return Long.SIZE / Byte.SIZE + numAttrs / Byte.SIZE + (numAttrs % Byte.SIZE == 0 ? 0 : 1);
    }

    public long getId() {
        return id;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public void setAttribute(int pos) {

        buffer[offset + pos / Byte.SIZE] |= 1 << (Byte.SIZE - (pos % Byte.SIZE + 1));
    }

    public void clearAttribute(int attrIndex) {

        buffer[offset + attrIndex / Byte.SIZE] &= ~(1 << (Byte.SIZE - (attrIndex % Byte.SIZE + 1)));
    }

    public boolean checkAttribute(int attrIndex) {

        return (buffer[offset + attrIndex / Byte.SIZE] >> (Byte.SIZE - (attrIndex % Byte.SIZE + 1)) & 1) == 1;
    }

    private static long readId(byte[] raw) {

        long result = 0;
        for (int i = 0; i < Byte.SIZE; i++) {
            result <<= Byte.SIZE;
            result |= (raw[i] & 0xFF);
        }
        return result;
    }

}
