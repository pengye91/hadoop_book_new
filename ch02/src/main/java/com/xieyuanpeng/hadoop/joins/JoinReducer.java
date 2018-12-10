package com.xieyuanpeng.hadoop.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    private Logger logger = LoggerFactory.getLogger(JoinReducer.class);

    @Override
    protected void reduce(Text custID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double total = 0.0;
        int count = 0;
        String name = "";

        for (Text value : values) {
            logger.info("[JoinReducer]: value \t" + value.toString());
            String[] fields = value.toString().split("\t");
            double amount;
            if (fields[0].equals("customer")) {
                name = fields[1];
            } else if (fields[0].equals("trans")) {
                amount = Double.parseDouble(fields[1]);
                count++;
                total += amount;
            }
        }

        context.write(new Text(name), new Text(Double.toString(total) + "," + Integer.toString(count)));
    }
}
