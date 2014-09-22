package com.dh.hadoopinpractice.ch1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Jebidia7
 * Date: 9/22/14
 * Time: 2:28 PM
 */
public class Map extends Mapper<LongWritable, Text, Text, Text> {

    private Text documentId;
    private Text word = new Text();

    @Override
    protected void setup(Context context) {

        documentId =
                new Text(((FileSplit)context.getInputSplit()).getPath().getName());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        for(String token : StringUtils.split(value.toString())) {

            word.set(token);
            context.write(word, documentId);
        }
    }
}