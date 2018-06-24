package com.c503.sc.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by YJH on 2018/6/15.
 */
public class WordCountStartConf {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration mrConf = new Configuration();
        mrConf.set("yjh","jd");
        String localPath = "E:\\BigDate\\code\\fileTest\\";
        Job job = Job.getInstance(mrConf);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(2);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(localPath + "WordCount.txt"));
        FileOutputFormat.setOutputPath(job, new Path(localPath + "wordCount"));
        job.waitForCompletion(true);
    }
}
