package kafka_connector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.ParseException;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.concurrent.TimeUnit;

public class BenchmarkMain {

    public static void main(String[] args) throws RunnerException {
        org.apache.commons.cli.Options opts = new org.apache.commons.cli.Options();
        Option benchmarkOption = Option.builder("b")
                .longOpt("benchmark")
                .hasArg()
                .optionalArg(false)
                .argName("benchmark name")
                .desc("choose benchmark to run")
                .build();
        Option forkOption = Option.builder("f")
                .longOpt("fork")
                .hasArg()
                .argName("fork number")
                .optionalArg(true)
                .desc("number of forks to run benchmark")
                .build();
        Option iterationOption = Option.builder("i")
                .longOpt("iteration")
                .hasArg()
                .type(Integer.class)
                .argName("iteration number")
                .optionalArg(true)
                .desc("number of iterations to run benchmark")
                .build();
        Option timeoutOption = Option.builder("t")
                .longOpt("timeout")
                .hasArg()
                .argName("timeout in seconds")
                .optionalArg(true)
                .desc("timeout of benchmark in seconds")
                .build();
        OptionGroup group = new OptionGroup();
        group.setRequired(true);
        group.addOption(benchmarkOption);
        opts.addOptionGroup(group);
        opts.addOption(forkOption);
        opts.addOption(iterationOption);
        opts.addOption(timeoutOption);
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(opts, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", opts);
            System.exit(1);
        }

        final String env = "local";
        final long time = System.currentTimeMillis();
        final String resultFile = String.format("jmh-results-%s-%s.json", env, time);
        final String outputFile = String.format("jmh-results-%s-%s.out", env, time);

        Options opt = new OptionsBuilder()
                .include(cmd.getOptionValue(benchmarkOption))
                .forks(1)
                .measurementIterations(Integer.parseInt(cmd.getOptionValue(iterationOption, "3")))
                .measurementTime(TimeValue.seconds(Long.parseLong(cmd.getOptionValue(timeoutOption, "15"))))
                .warmupIterations(1)
                .warmupTime(TimeValue.seconds(15))
                .mode(Mode.SampleTime)
                .timeUnit(TimeUnit.MILLISECONDS)
//                .addProfiler(GCProfiler.class)
//                .addProfiler(MemPoolProfiler.class)
                .jvmArgs("-Xms4g", "-Xmx4g")
                .resultFormat(ResultFormatType.JSON)
                .output(outputFile)
                .result(resultFile)
                .shouldFailOnError(true)
                .build();
        new Runner(opt).run();
    }
}
