package com.c503.sc.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * Created by YJH on 2018/6/15.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
//    Text word2 = new Text();
//    Random random = new Random();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String[] valueStr = value.toString().split(" ");
//        Configuration mrconf = context.getConfiguration();
//        if (null == valueStr) {
//            return;
//        }
//        for(String w1:valueStr){
//            word.set(w1);
//            context.write(word,one);
//        }
//        word.set(mrconf.get("yjh"));
//        word2.set("yjh" + random.nextInt(100));
//        context.write(word, word);
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()){
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
