package com.c503.sc.project1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collection;


public class Project1Start extends Dtinone_Hbase{

    private void listTables(Admin admin) throws Exception{
        if(admin != null){
            TableName[] list = admin.listTableNames();
            if(list != null && list.length > 0){
                for(TableName tableName : list){
                    System.out.println(tableName.getNameWithNamespaceInclAsString());
                    System.out.println(tableName.getNamespaceAsString() + ":" + tableName.getNameAsString());
                }
            }
        }
    }

    private  void createTable(Admin admin) throws Exception{
        if(admin != null){
//            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("dtinone"));
//            desc.addFamily(new HColumnDescriptor("cfn"));
//            desc.addFamily(new HColumnDescriptor("cfName").setTimeToLive(120));//120秒
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("area"));
            desc.addFamily(new HColumnDescriptor("info"));//120秒

//            admin.createTable(desc);

            //预分区
            String startKey = "rowKey-001";
            String endKey = "rowKey-112";
            int numRegins = 56;
            admin.createTable(desc, Bytes.toBytes(startKey), Bytes.toBytes(endKey), numRegins);
        }
    }

    private void disableTable(Admin admin) throws Exception{
        if(admin != null){
            admin.disableTable(TableName.valueOf("dtinone"));
            admin.disableTables("dtinone.*");
        }
    }

    private void enableTable(Admin admin) throws Exception{
        if(admin != null){
            admin.enableTable(TableName.valueOf("dtinone"));
            admin.enableTables("dtinone.*");
        }
    }

    private void modifyTable(Admin admin) throws Exception{
        if(admin != null){
            TableName tableName = TableName.valueOf("dtinone");
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor("cfn").setMaxVersions(3));
            desc.addFamily(new HColumnDescriptor("cfName").setTimeToLive(240));
            desc.addFamily(new HColumnDescriptor("cf00"));
            admin.modifyTable(tableName, desc);
        }
    }

    private void descriptor(Admin admin) throws  Exception{
        if(admin != null){
            HTableDescriptor desc = admin.getTableDescriptor(TableName.valueOf("dtinone"));
            System.out.println("Name:" + desc.getNameAsString());
            Collection<HColumnDescriptor> families = desc.getFamilies();
            if(families != null && !families.isEmpty()){
                for(HColumnDescriptor hdesc:families){
                    System.out.println("Families:" + hdesc.getNameAsString());
                }
            }
        }
    }

    private void deleteTable(Admin admin) throws Exception{
        if(admin != null){
            admin.disableTable(TableName.valueOf("area"));
            admin.deleteTable(TableName.valueOf("area"));

          /*  admin.disableTables("dtinone.*");
            admin.deleteTables("dtinone.*");*/
        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Project1Start project1Start = new Project1Start();
        Connection connec = project1Start.connectToHbase(hbaseMasterIPS,hbaseMasterPort,hbaseZnode);
        Admin admin = connec.getAdmin();
//        project1Start.createTable(admin);
//        project1Start.deleteTable(admin);

        TableName tn = TableName.valueOf("area");
        boolean isExists = admin.tableExists(tn);
        if(!isExists){//不存在表则建立
            project1Start.createTable(connec.getAdmin());
        }else {//存在则删除再建
            project1Start.deleteTable(connec.getAdmin());
            project1Start.createTable(connec.getAdmin());
        }

        String hdfsPath = "hdfs://dtinone:8020/user/hadoop/dtinone/";
        Job job = Job.getInstance();
        job.setJobName("project1_job");
        job.setMapperClass(Project1Mapper.class);
//        job.setReducerClass(Project1Reducer.class);
        job.setOutputKeyClass(Text.class);//同时设置map和reduce的输出
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(hdfsPath + "area.json"));

        Path outpath = new Path(hdfsPath + "out.txt");
        FileSystem fileSystem = outpath.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(outpath)) {
            fileSystem.delete(outpath, true);// true的意思是，就算output有东西，也一带删除
        }

        //ASK：为什么必须设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(hdfsPath + "out.txt"));
//        job.submit();
        job.waitForCompletion(true);
        System.out.println("job submit finished");
    }
}
