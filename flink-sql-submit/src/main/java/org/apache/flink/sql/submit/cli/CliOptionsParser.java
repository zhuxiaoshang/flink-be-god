package org.apache.flink.sql.submit.cli;

import org.apache.commons.cli.*;

/**
 * @author: zhushang
 * @create: 2020-11-05 17:06
 */
public class CliOptionsParser {
    public static final Option OPTION_WORKING_SPACE =
            Option.builder("w")
                    .required(true)
                    .longOpt("working_space")
                    .numberOfArgs(1)
                    .argName("working space dir")
                    .desc("The working space dir.")
                    .build();

    public static final Option OPTION_SQL_FILE =
            Option.builder("f")
                    .required(true)
                    .longOpt("file")
                    .numberOfArgs(1)
                    .argName("SQL file path")
                    .desc("The SQL file path.")
                    .build();

    public static final Option JOB_NAME =
            Option.builder("job_name")
                    .required(false)
                    .longOpt("job name")
                    .numberOfArgs(1)
                    .argName("job name")
                    .desc("job name.")
                    .build();

    public static final Option OPTION_UDF_JAR =
            Option.builder("udf")
                    .required(false)
                    .longOpt("udf jars")
                    .numberOfArgs(Option.UNLIMITED_VALUES)
                    .argName("UDF jar file")
                    .desc("UDF jar file")
                    .build();

    public static final Options CLIENT_OPTIONS = getClientOptions(new Options());

    public static Options getClientOptions(Options options) {
        options.addOption(OPTION_SQL_FILE);
        options.addOption(OPTION_WORKING_SPACE);
        options.addOption(JOB_NAME);
        options.addOption(OPTION_UDF_JAR);
        return options;
    }

    // --------------------------------------------------------------------------------------------
    //  Line Parsing
    // --------------------------------------------------------------------------------------------

    public static CliOptions parseClient(String[] args) {
        if (args.length < 1) {
            throw new RuntimeException("./sql-submit -w <work_space_dir> -f <sql-file>");
        }
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(CLIENT_OPTIONS, args, true);
            return new CliOptions(
                    line.getOptionValue(CliOptionsParser.OPTION_SQL_FILE.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_WORKING_SPACE.getOpt()),
                    line.getOptionValue(CliOptionsParser.JOB_NAME.getOpt()),
                    line.getOptionValues(CliOptionsParser.OPTION_UDF_JAR.getOpt()));
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
