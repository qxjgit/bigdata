package com.c503.sc.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by YJH on 2018/6/15.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//    private static String count = null;
//    Text text = new Text();
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       int sum = 0;
       for(IntWritable val:values){
        sum += val.get();
       }
       result.set(sum);
        context.write(key, result);

//        for (Text v : values) {
//            count = count + v.toString();
//        }
//        text.set(count);
//        context.write(key, text);
    }

}
