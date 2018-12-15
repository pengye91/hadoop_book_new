package com.xieyuanpeng.hadoop.book.ch03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;

/**
 * @author xyp
 * @date 18-12-15 下午11:47
 */
public class FileCopyWithProgress {
    private static Logger logger = LoggerFactory.getLogger(FileCopyWithProgress.class);
    public static void main(String[] args) throws IOException {
        String src = args[0];
        String dst = args[1];

        InputStream in = new BufferedInputStream(new FileInputStream(src));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);

        OutputStream out = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                logger.info(".");
            }
        });

        IOUtils.copyBytes(in, out, 4096, true);
    }
}
