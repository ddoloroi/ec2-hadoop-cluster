/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.    See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.    The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Runs a job multiple times and takes average of all runs.
 */
public class MRBench {

    private static final Log LOG = LogFactory.getLog(MRBench.class);
    private static Path BASE_DIR =
        new Path(System.getProperty("test.build.data","/benchmarks/MRBench"));
    private static Path INPUT_DIR = new Path(BASE_DIR, "mr_input");
    private static Path OUTPUT_DIR = new Path(BASE_DIR, "mr_output");

    public static enum Order {RANDOM, ASCENDING, DESCENDING};

    /**
     * Takes input format as text lines, runs some processing on it and
     * writes out data as text again.
     */
    public static class Map
        extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce
        extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    /**
     * Generate a text file on the given filesystem with the given path name.
     * The text file will contain the given number of lines of generated data.
     * The generated data are string representations of numbers.    Each line
     * is the same length, which is achieved by padding each number with
     * an appropriate number of leading '0' (zero) characters.    The order of
     * generated data is one of ascending, descending, or random.
     */
    public static void generateTextFile(FileSystem fs, Path inputFile,
            long numLines, Order sortOrder) throws IOException
    {
        LOG.info("creating control file: "+numLines+" numLines, "+sortOrder+" sortOrder");
        PrintStream output = null;
        try {
            output = new PrintStream(fs.create(inputFile));
            int padding = String.valueOf(numLines).length();
            switch(sortOrder) {
            case RANDOM:
                for (long l = 0; l < numLines; l++) {
                    output.println(pad((new Random()).nextLong(), padding));
                }
                break;
            case ASCENDING:
                for (long l = 0; l < numLines; l++) {
                    output.println(pad(l, padding));
                }
                break;
            case DESCENDING:
                for (long l = numLines; l > 0; l--) {
                    output.println(pad(l, padding));
                }
                break;
            }
        } finally {
            if (output != null)
                output.close();
        }
        LOG.info("created control file: " + inputFile);
    }

    /**
     * Convert the given number to a string and pad the number with
     * leading '0' (zero) characters so that the string is exactly
     * the given length.
     */
    private static String pad(long number, int length) {
        String str = String.valueOf(number);
        StringBuffer value = new StringBuffer();
        for (int i = str.length(); i < length; i++) {
            value.append("0");
        }
        value.append(str);
        return value.toString();
    }

    /**
     * Create the job configuration.
     */
    private static Job setupJob() throws Exception {
        Random randomGenerator = new Random();
        Configuration conf = new Configuration();
        conf.set("deadline", new Integer(randomGenerator.nextInt(100)).toString());
        Job job = Job.getInstance(conf, "mrbench: word count");
        job.setJarByClass(MRBench.class);
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, INPUT_DIR);
        return job;
    }

    /**
     * Runs a MapReduce task, given number of times. The input to each run
     * is the same file.
     */
    private static ArrayList<Job> runJobInSequence(int numRuns) throws Exception {
        ArrayList<Job> jobs = new ArrayList<>();

        for (int i = 0; i < numRuns; i++) {
            Job job = setupJob();
            jobs.add(job);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR, "output_" + i));

            LOG.info("Running job " + i + ":" +
                    " input=" + FileInputFormat.getInputPaths(job)[0] +
                    " output=" + FileOutputFormat.getOutputPath(job));

            LOG.info("[ID] " + job.ID + " / " + job.TASK_ID);
            // run the mapred task now
            job.submit();
        }

        for (int i = 0; i < numRuns; i++) {
            while (!jobs.get(i).isComplete()) {
                Thread.sleep(300);
            }
        }
        return jobs;
    }

    public static void main (String[] args) throws Exception {
        String version = "MRBenchmark.0.0.2";
        System.out.println(version);

        String usage =
            "Usage: mrbench " +
            "[-baseDir <base DFS path for output/input, default is /benchmarks/MRBench>] " +
            "[-jar <local path to job jar file containing Mapper and Reducer implementations, default is current jar file>] " +
            "[-numRuns <number of times to run the job, default is 1>] " +
            "[-inputLines <number of input lines to generate, default is 1>] " +
            "[-inputType <type of input to generate, one of ascending (default), descending, random>] " +
            "[-verbose]";

        long inputLines = 1;
        int numRuns = 1;
        boolean verbose = false;
        Order inputSortOrder = Order.ASCENDING;
        for (int i = 0; i < args.length; i++) { // parse command line
            if (args[i].equals("-numRuns")) {
                numRuns = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-baseDir")) {
                BASE_DIR = new Path(args[++i]);
            } else if (args[i].equals("-inputLines")) {
                inputLines = Long.parseLong(args[++i]);
            } else if (args[i].equals("-inputType")) {
                String s = args[++i];
                if (s.equalsIgnoreCase("ascending")) {
                    inputSortOrder = Order.ASCENDING;
                } else if (s.equalsIgnoreCase("descending")) {
                    inputSortOrder = Order.DESCENDING;
                } else if (s.equalsIgnoreCase("random")) {
                    inputSortOrder = Order.RANDOM;
                } else {
                    inputSortOrder = null;
                }
            } else if (args[i].equals("-verbose")) {
                verbose = true;
            } else {
                System.err.println(usage);
                System.exit(-1);
            }
        }

        if (numRuns < 1 ||    // verify args
                inputLines < 0 ||
                inputSortOrder == null)
            {
                System.err.println(usage);
                System.exit(-1);
            }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path(INPUT_DIR, "input_" + (new Random()).nextInt() + ".txt");
        generateTextFile(fs, inputFile, inputLines, inputSortOrder);

        // setup test output directory
        fs.mkdirs(BASE_DIR);
        ArrayList<Job> jobs = new ArrayList<Job>();
        try {
            jobs = runJobInSequence(numRuns);
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            // delete output -- should we really do this?
            fs.delete(BASE_DIR, true);
        }

        if (verbose) {
            // Print out a report
            System.out.println("Total MapReduce jobs executed: " + numRuns);
            System.out.println("Total lines of data per job: " + inputLines);
        }
        int i = 0;
        long totalTime = 0;
        long avgTime = totalTime / numRuns;
        System.out.println("DataLines\tAvgTime (milliseconds)");
        System.out.println(inputLines + "\t\t" + avgTime);
        System.exit(0);
    }
}
