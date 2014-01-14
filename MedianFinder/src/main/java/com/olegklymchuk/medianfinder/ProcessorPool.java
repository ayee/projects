package com.olegklymchuk.medianfinder;

//import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class ProcessorPool {

    private static final ProcessorPool self = new ProcessorPool();
    private java.util.Map<String, Processor> processors = new java.util.HashMap<String, Processor>();

    private ProcessorPool() {

        try {

            processors.put("-inline", new InlineProcessor());
            processors.put("-inputgen", new InputGenerator());
            processors.put("-mr", new MFJob<SequenceFileInputFormat, MFMapper, MFReducer>(SequenceFileInputFormat.class, MFMapper.class, MFReducer.class));
            processors.put("-mrs", new MFJob<SequenceFileInputFormat, MFSimpleMapper, MFReducer>(SequenceFileInputFormat.class, MFSimpleMapper.class, MFReducer.class));
        }
        catch (java.io.IOException ex) {

            System.out.println("ProcessorPool.ProcessorPool(): failed to create processor due to error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    public static ProcessorPool instance() {

        return self;
    }

    public Processor getProcessor(String key) throws RuntimeException {

        if (!processors.containsKey(key))
            throw new RuntimeException("Failed to find processor for specified key.");

        return processors.get(key);
    }
}
