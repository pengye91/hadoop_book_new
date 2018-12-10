package com.xieyuanpeng.hadoop.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author xyp
 */
public class CustMapper extends Mapper<Object, Text, Text, Text> {
    private static int CORRECT_NUM = 5;
    private Logger logger = Logger.getLogger(CustMapper.class);

    @Override
    public void map(Object key, Text customerRecord, Context context) throws IOException, InterruptedException {
        String[] customerFields = customerRecord.toString().split(",");
        int custFieldsNum = customerFields.length;
        String custID = "";

        System.out.println(custFieldsNum);

        if (custFieldsNum == CORRECT_NUM) {
            custID = customerFields[0];
            logger.info("[CustMapper] info: custID: " + custID);
        } else {
            logger.error("[CustMapper] error: customerFieldsNum < 5");
            System.exit(1);
        }

        context.write(new Text(custID), new Text("customer\t" + customerFields[1]));
    }
}
