package ok.gd.counters;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by olegklymchuk on 8/20/16.
 *
 */

public class DatasetGenerator {

    private static ByteBuffer idBuffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);


    public static void generateRecords(String outFilePath, int numAttrs, long numRecords, int[] bitsToSet, boolean appendRecords) throws IOException {

        Random idGenerator = new Random();

        Record record = new Record(0, numAttrs);
        for(int i : bitsToSet) {
            record.setAttribute(i);
        }

        byte[] attributes = record.getBuffer();

        idBuffer.clear();

        try(FileOutputStream fileOutputStream = new FileOutputStream(outFilePath, appendRecords)) {
            for (int i = 0; i < numRecords; ++i) {
                fileOutputStream.write(idBuffer.putLong(0, idGenerator.nextLong()).array());
                fileOutputStream.write(attributes);
            }
        }
    }
}
