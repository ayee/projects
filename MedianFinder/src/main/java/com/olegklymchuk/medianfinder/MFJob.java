package com.olegklymchuk.medianfinder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// TODO: call MedianFinder.getMedian() on job completion to calculate the median

public class MFJob<IFC extends org.apache.hadoop.mapreduce.InputFormat,
                   MC extends org.apache.hadoop.mapreduce.Mapper,
                   RC extends org.apache.hadoop.mapreduce.Reducer> extends Configured implements Tool, Processor {

    static {

        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    private Class<IFC> ifcClass;
    private Class<MC> mcClass;
    private Class<RC> rcClass;

    public MFJob(Class<IFC> ifcClass, Class<MC> mcClass, Class<RC> rcClass) {

        this.ifcClass = ifcClass;
        this.mcClass = mcClass;
        this.rcClass = rcClass;
    }

    public int process(String[] args) throws Exception {

        return ToolRunner.run(this, args);
    }

    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();

        if (strings.length < 3) {

            System.out.println("INVALID NUMBER OF INPUT PARAMETERS");

            return -1;
        }

        Job job = new Job(conf, "MFJob");

        job.setJarByClass(App.class);
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        job.setMapperClass(mcClass);
        job.setInputFormatClass(ifcClass);
        job.setReducerClass(rcClass);

        job.setReducerClass(MFReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        int result = (job.waitForCompletion(true) ? 0 : 1);

        return result;
    }

}
