package com.xieyuanpeng.hadoop.book.ch02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xyp
 * @date 2018/11/21
 **/
public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lineWords = value.toString().split("\\s+");
        String year = lineWords[0];
        int airTemp = Integer.parseInt(lineWords[4]);
        if (airTemp != MISSING) {
            context.write(new Text(year), new IntWritable(airTemp));
        }
    }
}

