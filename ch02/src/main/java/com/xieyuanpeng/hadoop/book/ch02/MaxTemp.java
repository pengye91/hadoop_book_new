package com.xieyuanpeng.hadoop.book.ch02;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * @author xyp
 * @date 2018/12/01
 **/
public class MaxTemp {
    private static Logger logger = Logger.getLogger(MaxTemp.class);
    private static final int ARGS_LENGTH = 2;

    public static void main(String[] args) throws Exception {
        if (args.length != ARGS_LENGTH) {
            logger.error("Usage: MaxTemp <input path> <output path>");
            System.exit(1);
        }

        JobConf jobConf = new JobConf();

        Job job = Job.getInstance(jobConf);
        job.setJarByClass(MaxTemp.class);
        job.setJobName("MaxTemp Job");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTempMapper.class);
        job.setReducerClass(MaxTempReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
