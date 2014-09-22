package com.dh.hadoopinpractice.ch1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: Jebidia7
 * Date: 9/22/14
 * Time: 2:36 PM
 */
public class MRdriver {

    public static void main(String[] args) throws Exception {

        runJob(
                Arrays.copyOfRange(args, 0, args.length-1),
                args[args.length-1]);
    }

    private static void runJob(String[] input, String output)
            throws Exception {

        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(MRdriver.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(configuration).delete(outputPath, true);

        job.waitForCompletion(true);
    }
}
