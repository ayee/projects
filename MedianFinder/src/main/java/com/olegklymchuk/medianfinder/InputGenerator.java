package com.olegklymchuk.medianfinder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.Random;


public class InputGenerator implements Processor {

    private static final int MAX_FILES = 10;
    private static final int MAX_NUMBERS_IN_FILE = 40;
    private static final int MAX_RAND = 1001;

    private Random rand = new Random();

    private IntWritable value = new IntWritable();
    private Configuration conf = new Configuration();
    private LocalFileSystem localFS = null;


    public InputGenerator() throws IOException {

        localFS = LocalFileSystem.getLocal(conf);
    }

    public int process(String[] args) throws IOException {

        return generateInputFiles(args[1]);
    }

    private int generateInputFiles(String destDir) {

        try {

            for (int i = 0; i < MAX_FILES; ++i) {

                SequenceFile.Writer writer = SequenceFile.createWriter(localFS, conf, new Path(destDir + "/" + i + ".seq"), IntWritable.class, NullWritable.class);

                for (int j = 0; j < MAX_NUMBERS_IN_FILE; ++j) {

                    value.set(rand.nextInt(MAX_RAND));
                    writer.append(value, NullWritable.get());
                }

                IOUtils.closeStream(writer);
            }

        }
        catch (IOException ex) {

            System.out.println("InputGenerator.generateInputFiles() failed due to error: " + ex.getMessage());

            return -1;
        }

        return 0;
    }
}
