package com.xiaodong.hadoop.recommend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by xiaodong on 2016/7/10.
 */
public class RecommendReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> set = new HashSet<String>();
        for (Text value:values) {
            set.add(value.toString());
        }
        if (set.size() > 1) {
            for (String name:set) {
                for (String other:set) {
                    if (!name.equals(other)) {
                        context.write(new Text(name), new Text(other));
                    }
                }
            }
        }
    }
}
