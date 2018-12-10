package com.xieyuanpeng.hadoop.joins;

import com.xieyuanpeng.hadoop.book.ch02.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author xyp
 */


public class ReduceSideJoin {
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        Job job = Job.getInstance(conf, "Reduce-side join");
        job.setJarByClass(ReduceSideJoin.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransMapper.class);
        Path outputPath = Utils.mustOutputPath(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

