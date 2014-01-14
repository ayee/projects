package com.olegklymchuk.medianfinder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.TreeMap;

public class KeyValueReader {

    public static TreeMap readFromFiles(String srcDir) {

        TreeMap<Integer, Integer> tm = new TreeMap<Integer, Integer>();

        try {

            Configuration conf = new Configuration();
            LocalFileSystem localFS = LocalFileSystem.getLocal(conf);

            File src = new File(srcDir);
            if (!src.isDirectory())
                return readFromFile(tm, src, conf, localFS);

            File[] files = src.listFiles(new FilenameFilter() { public boolean accept(File dir, String fileName) {return fileName.endsWith("part-r-*");}});
            for (File f : files)
                readFromFile(tm, f, conf, localFS);
        }
        catch (IOException ex) {

            System.out.println("KeyValueReader.readFromFiles(): failed to read key/value pairs from file due to error: " + ex.getMessage());
            ex.printStackTrace();
        }

        return tm;
    }

    public static TreeMap readFromFile(TreeMap<Integer, Integer> tm, File file, Configuration conf, LocalFileSystem localFS) throws IOException {

        SequenceFile.Reader reader = null;

        try {

            reader = new SequenceFile.Reader(localFS, new Path(file.getAbsolutePath()), conf);
            Writable  key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value))
                tm.put(Integer.parseInt(key.toString()), Integer.parseInt(value.toString()));
        }
        finally {

            IOUtils.closeStream(reader);
        }

        return tm;
    }
}
