package com.c503.sc.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by YJH on 2018/6/15.
 */
//com.c503.hadoop.mapreduce.mrLen.WordCountStart
public class WordCountStart {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String localPath = "G:\\bigdata\\";
        String hdfsPath = "hdfs://c503/yjhTest/";

        Job job = Job.getInstance();
        job.setJobName("yjh-word-Count");
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);//同时设置map和reduce的输出
        job.setMapOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(localPath + "WordCount.txt"));
//        FileOutputFormat.setOutputPath(job, new Path(localPath + "wordCount"));
        FileInputFormat.addInputPath(job, new Path(localPath + "test.txt"));
        FileOutputFormat.setOutputPath(job, new Path(localPath + "out"));
//        job.submit();
        job.waitForCompletion(true);
        System.out.println("job submit finished");
    }
}
