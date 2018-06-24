package com.c503.sc.qiuxj;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Dtinone_CreateSqlMap {
    public static class CreateSqlMapper extends Mapper<LongWritable, Text, Text, Text>{
        public  void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
            String[] values = value.toString().split("\t");
            if(value != null && values.length >= 6){
                String userId = values[0];
                String user = values[1];
                String location = values[2];
                String post = values[3];
                String dt = values[4];
                String dta11 = values[5];

                String valueStr = "'" + userId + "','"+user+"','"+location+"','"+post+"','"+dt+"','"+dta11+"'";
                context.write(new Text("sql"),new
                Text("insert into dtinone(userId, user, location, post, dt, dtall) values("+valueStr+");"));
            }

        }
    }
}
