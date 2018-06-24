package com.c503.sc.qiuxj;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Dtinone_CreateSqlReducer {
    public void reduce(Text key,Iterable<Text> values, Reducer.Context context) throws IOException,InterruptedException{
        if (values != null){
            for(Text value:values){
                context.write(new Text(""), new Text(value.toString()));
            }
        }
    }
}
