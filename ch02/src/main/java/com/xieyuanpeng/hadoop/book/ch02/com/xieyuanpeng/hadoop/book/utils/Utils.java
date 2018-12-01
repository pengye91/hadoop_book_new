package com.xieyuanpeng.hadoop.book.ch02.com.xieyuanpeng.hadoop.book.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Date;

/**
 * @author xyp
 */
public class Utils {

    public static Path mustOutputPath(String rawPathUri) throws IOException {
        Path rawPath = new Path(rawPathUri);
        Date nowDate = new Date();
        String newPath = rawPathUri + "_" + Long.toString(nowDate.getTime());
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(rawPath)) {
            fs.rename(rawPath, new Path(newPath));
        }
        return rawPath;
    }

}
