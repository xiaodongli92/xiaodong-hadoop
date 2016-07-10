package com.xiaodong.hadoop.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by xiaodong on 2016/7/9.
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 每次调用map方法会传入split中一行数据key：该行数据所在文件中的位置下标，value：这行行数据
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer stringTokenizer = new StringTokenizer(line);
        while (stringTokenizer.hasMoreTokens()) {
            String world = stringTokenizer.nextToken();
            context.write(new Text(world), new IntWritable(1));
        }
    }
}
