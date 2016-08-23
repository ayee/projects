package ok.gd.counters;

import org.apache.commons.cli.*;

/**
 * Created by olegklymchuk on 8/21/16.
 *
 */

public class Main {

    private static final String PATH = "path";
    private static final String NUM_ATTRS = "attrs";
    private static final String ALL = "all";
    private static final String ANY = "any";
    private static final String NONE = "none";
    private static final String NUM_THREADS = "threads";

    private static final Options options = new Options();
    private static final HelpFormatter helpFormatter = new HelpFormatter();

    static {
        Option path = OptionBuilder.hasArg().withDescription("Path to the dataset file or folder").isRequired().create(PATH);
        Option numAttrs = OptionBuilder.hasArg().withDescription("Number of attributes in the record").isRequired().create(NUM_ATTRS);
        Option all = OptionBuilder.hasArg().withDescription("Comma-separated list of zero-based indices. All attributes referenced by these indices must be set to '1' in order to pass the filter.").create(ALL);
        all.setValueSeparator(',');
        all.setArgs(Option.UNLIMITED_VALUES);
        Option any = OptionBuilder.hasArg().withDescription("Comma-separated list of zero-based indices. At least one of the attributes referenced by these indices must be set to '1' in order to pass the filter.").create(ANY);
        any.setValueSeparator(',');
        any.setArgs(Option.UNLIMITED_VALUES);
        Option none = OptionBuilder.hasArg().withDescription("Comma-separated list of zero-based indices. All attributes referenced by these indices must be set to '0' in order to pass the filter.").create(NONE);
        none.setValueSeparator(',');
        none.setArgs(Option.UNLIMITED_VALUES);
        Option numThreads = OptionBuilder.hasArg().withDescription("Number of threads to create for the processing.").create(NUM_THREADS);

        options.addOption(path);
        options.addOption(numAttrs);
        options.addOption(all);
        options.addOption(any);
        options.addOption(none);
        options.addOption(numThreads);
    }


    public static void main( String[] args ) {

        CommandLineParser parser = new BasicParser();
        CommandLine cmd;
        try {

            cmd = parser.parse(options, args, true);

        } catch (ParseException e) {

            helpFormatter.printHelp("filter -path \"/path/to/the/dataset\" -all \"0,2,7,8\" -any \"12,13\" -none \"5\" -threads 60", options);

            return;
        }

        String path = cmd.getOptionValue(PATH);
        int numAttrs = Integer.parseInt(cmd.getOptionValue(NUM_ATTRS));
        String numThreadsParam = cmd.getOptionValue(NUM_THREADS);
        int numThreads;
        if(numThreadsParam != null && !numThreadsParam.isEmpty()) {
            numThreads = Integer.parseInt(numThreadsParam);
        } else {
            numThreads = Runtime.getRuntime().availableProcessors();
        }

        FilterCondition filterCondition = new FilterCondition(toIntArray(cmd.getOptionValues(ALL)),
                                                                toIntArray(cmd.getOptionValues(ANY)),
                                                                toIntArray(cmd.getOptionValues(NONE)));

        try (ConditionalCounter counter = new ConditionalCounter(path, numAttrs, filterCondition, numThreads)) {

            long start = System.currentTimeMillis();
            long count = counter.count();
            long totalTime = System.currentTimeMillis() - start;
            System.out.println("Time Spent: " + totalTime);

            System.out.println(count);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int[] toIntArray(String[] strArray) {

        if(strArray == null || strArray.length == 0) {
            return null;
        }

        int[] result = new int[strArray.length];
        for(int i = 0; i < strArray.length; ++i) {
            result[i] = Integer.parseInt(strArray[i]);
        }

        return result;
    }

}
