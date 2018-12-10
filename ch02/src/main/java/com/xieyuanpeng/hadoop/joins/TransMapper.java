package com.xieyuanpeng.hadoop.joins;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author xyp
 */
public class TransMapper extends Mapper<Object, Text, Text, Text> {
    private static int CORRECT_FIELDS_NUM = 9;
    private Logger logger = Logger.getLogger(TransMapper.class);

    @Override
    public void map(Object key, Text transFieldsRecord, Context context) throws IOException, InterruptedException {
        String[] transFields = transFieldsRecord.toString().split(",");
        if (transFields.length != CORRECT_FIELDS_NUM) {
            logger.error("[TransMapper] error: TransFields number != 9");
            System.exit(1);
        } else {
            String custID = transFields[2];
            String amount = transFields[3];

            logger.info("[TransMapper] info: custID: " + custID);
            context.write(new Text(custID), new Text("trans\t" + amount));
        }
    }
}
