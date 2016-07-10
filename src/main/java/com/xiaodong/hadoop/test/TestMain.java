package com.xiaodong.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by xiaodong on 2016/7/6.
 */
public class TestMain {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.set("mapred.job.tracker","node1:9001");
        try {
            Job job = new Job(configuration);
            job.setJarByClass(TestMain.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(1);

            //mapReduce 输入数据所在目录活文件
            FileInputFormat.addInputPath(job, new Path("/user/root/input/my/"));
            //mapReduce 执行之后输出数据的目录
            FileOutputFormat.setOutputPath(job, new Path("/opt/output/my/"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
