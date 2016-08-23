package ok.gd.counters;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by olegklymchuk on 8/22/16.
 *
 */

public class ConditionalCounterJob  extends Configured implements Tool {

    public static final String INPUT_PATH = "in_path";
    public static final String OUTPUT_PATH = "out_path";

    private static final Options options = new Options();
    private static final HelpFormatter helpFormatter = new HelpFormatter();

    static {
        Option path = OptionBuilder.hasArg().withDescription("Path to the dataset file or folder").isRequired().create(INPUT_PATH);
        Option numAttrs = OptionBuilder.hasArg().withDescription("Number of attributes in the record").isRequired().create(ConditionalCounterMapper.NUM_ATTRS);
        Option all = OptionBuilder.hasArg().withDescription("Comma-separated list of zero-based indices. All attributes referenced by these indices must be set to '1' in order to pass the filter.").create(ConditionalCounterMapper.ALL);
        all.setValueSeparator(',');
        all.setArgs(Option.UNLIMITED_VALUES);
        Option any = OptionBuilder.hasArg().withDescription("Comma-separated list of zero-based indices. At least one of the attributes referenced by these indices must be set to '1' in order to pass the filter.").create(ConditionalCounterMapper.ANY);
        any.setValueSeparator(',');
        any.setArgs(Option.UNLIMITED_VALUES);
        Option none = OptionBuilder.hasArg().withDescription("Comma-separated list of zero-based indices. All attributes referenced by these indices must be set to '0' in order to pass the filter.").create(ConditionalCounterMapper.NONE);
        none.setValueSeparator(',');
        none.setArgs(Option.UNLIMITED_VALUES);

        options.addOption(path);
        options.addOption(numAttrs);
        options.addOption(all);
        options.addOption(any);
        options.addOption(none);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = updateConfiguration(getConf(), args);

        Job job = Job.getInstance(conf, ConditionalCounterJob.class.getSimpleName());

        job.setJarByClass(ConditionalCounterJob.class);

        FileInputFormat.addInputPath(job, new Path(conf.get(INPUT_PATH)));

        job.setMapperClass(ConditionalCounterMapper.class);

        job.setReducerClass(ConditionalCounterReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(BytesWritable.class);

        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(conf.get(OUTPUT_PATH)));

        if(job.waitForCompletion(true)) {
            return 0;
        }

        return -1;
    }

    protected Configuration updateConfiguration(Configuration conf, String[] args) {

        CommandLineParser parser = new BasicParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args, true);
        } catch (ParseException e) {

            helpFormatter.printHelp("filter -path \"/path/to/the/dataset\" -all \"0,2,7,8\" -any \"12,13\" -none \"5\"", options);

            throw new RuntimeException("Not enough params to run the job");
        }

        conf.set(INPUT_PATH, cmd.getOptionValue(INPUT_PATH));
        conf.set(OUTPUT_PATH, cmd.getOptionValue(OUTPUT_PATH));
        conf.setInt(ConditionalCounterMapper.NUM_ATTRS, Integer.parseInt(cmd.getOptionValue(ConditionalCounterMapper.NUM_ATTRS)));
        conf.set(ConditionalCounterMapper.ALL, cmd.getOptionValue(ConditionalCounterMapper.ALL));
        conf.set(ConditionalCounterMapper.ANY, cmd.getOptionValue(ConditionalCounterMapper.ANY));
        conf.set(ConditionalCounterMapper.NONE, cmd.getOptionValue(ConditionalCounterMapper.NONE));

        return conf;
    }

    public static void main(String[] args) throws Exception {

        ConditionalCounterJob aaeDataProcessor = new ConditionalCounterJob();

        int exitCode = ToolRunner.run(aaeDataProcessor, args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }
}
