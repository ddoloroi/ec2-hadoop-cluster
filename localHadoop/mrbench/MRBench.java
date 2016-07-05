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
    private static String[] files;
    private static int[] deadlines;

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
     * Create the job configuration.
     */
    private static Job setupJob(int deadline) throws Exception {
        Random randomGenerator = new Random();
        Configuration conf = new Configuration();
        conf.set("deadline", new Integer(deadline).toString());
        Job job = Job.getInstance(conf, "mrbench: word count");
        job.setJarByClass(MRBench.class);
        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job;
    }

    /**
     * Runs a MapReduce task, given number of times. The input to each run
     * is the same file.
     */
    private static ArrayList<Job> runJobInSequence(int numRuns) throws Exception {
        ArrayList<Job> jobs = new ArrayList<>();

        for (int i = 0; i < numRuns; i++) {
            Job job;
            if (deadlines.length == numRuns) {
                job = setupJob(deadlines[i]);
            } else {
                int index = (int)Math.floor(Math.random() * deadlines.length);
                job = setupJob(deadlines[index]);
            }

            if (files.length == numRuns) {
                FileInputFormat.addInputPath(job, new Path(INPUT_DIR, files[i]));
                job.setJobName(files[i]);
            } else {
                int index = (int)Math.floor(Math.random() * files.length);
                FileInputFormat.addInputPath(job, new Path(INPUT_DIR, files[index]));
                job.setJobName(files[index]);
            }

            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR, "output_" + i));
            jobs.add(job);

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
            "[-inputFiles </file1,/file2/file3,etc>] " +
            "[-deadlines <d1,d2,d3,etc>] " +
            "[-verbose]";

        int numRuns = 1;
        boolean verbose = false;
        for (int i = 0; i < args.length; i++) { // parse command line
            if (args[i].equals("-numRuns")) {
                numRuns = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-baseDir")) {
                BASE_DIR = new Path(args[++i]);
            } else if (args[i].equals("-inputFiles")) {
                files = args[++i].split(",");
            } else if (args[i].equals("-deadlines")) {
                String[] s = args[++i].split(",");
                deadlines = new int[s.length];
                for (int j = 0; j < s.length; j++) {
                    deadlines[j] = Integer.parseInt(s[j]);
                }
            } else if (args[i].equals("-verbose")) {
                verbose = true;
            } else {
                System.err.println(usage);
                System.exit(-1);
            }
        }

        if (numRuns < 1 ||    // verify args
                files == null ||
                deadlines == null)
            {
                System.err.println(usage);
                System.exit(-1);
            }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path(INPUT_DIR, "input_" + (new Random()).nextInt() + ".txt");

        // setup test output directory
        fs.mkdirs(BASE_DIR);
        ArrayList<Job> jobs = new ArrayList<Job>();
        try {
            jobs = runJobInSequence(numRuns);
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            // delete output -- should we really do this?
            fs.delete(OUTPUT_DIR, true);
        }

        if (verbose) {
            // Print out a report
            System.out.println("Total MapReduce jobs executed: " + numRuns);
        }
        int i = 0;
        System.out.println("JonName,runTime,deadline,slack,weightedSlack");
        for (Job job : jobs) {
            long runTime = (job.getFinishTime() - job.getStartTime()) / 1000;
            long deadline = (long)deadlines[i];
            long slack = deadline > runTime ? 0 : runTime - deadline;
            long weightedSlack = (long)((float)slack / deadline * 100);
            System.out.println(String.format("%s,%d,%d,%d,%d", job.getJobID(), runTime, deadline, slack, weightedSlack));
            i += 1;
        }
        System.exit(0);
    }
}
