package com.c503.sc.project1;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.alibaba.fastjson.JSON;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.StringTokenizer;

import static com.c503.sc.project1.Dtinone_Hbase.hbaseMasterIPS;
import static com.c503.sc.project1.Dtinone_Hbase.hbaseMasterPort;
import static com.c503.sc.project1.Dtinone_Hbase.hbaseZnode;

/**
 * Created by YJH on 2018/6/15.
 */
public class Project1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    int PutNum = 10;
    Connection conn = null;
    ArrayList<Put> putList =   new ArrayList<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);
        Dtinone_Hbase dtinone_hbase= new Dtinone_Hbase();
        Connection connect = dtinone_hbase.connectToHbase(hbaseMasterIPS,hbaseMasterPort,hbaseZnode);
        this.conn = connect;
    }


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Table table = this.conn.getTable(TableName.valueOf("area"));
        //把一行json数据解析为字符串数组
        JSONObject json = JSONObject.parseObject(value.toString());
        String areaId = json.getString("areaId");
        String parentId = json.getString("parentId");
        String areaName = json.getString("areaName");

        Put put = new Put(Bytes.toBytes(parentId+areaId));
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("areaId"),objectToByte(json.getString("areaId")));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("areaId"),Bytes.toBytes(areaId));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("parentId"),Bytes.toBytes(parentId));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("areaName"),Bytes.toBytes(areaName));
        this.putList.add(put);
        if (this.putList.size() >= PutNum) {
            table.put(this.putList);
            this.putList.clear();
        }
    }

    protected byte[] objectToByte(Object obj) {
    byte[] bytes = null;
    if(obj.getClass() == String.class){
        return ((String) obj).getBytes();
    }
    try  {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ObjectOutputStream oo = new ObjectOutputStream(bo);
        oo.writeObject(obj);

        bytes = bo.toByteArray();

        bo.close();
        oo.close();
    }
    catch(Exception e){
        System.out.println("translation"+e.getMessage());
        e.printStackTrace();
    }
    return bytes;
}
}
