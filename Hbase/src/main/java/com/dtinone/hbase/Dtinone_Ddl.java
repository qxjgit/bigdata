package com.dtinone.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Collection;


public class Dtinone_Ddl extends Dtinone_Hbase{

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
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("logs"));
            desc.addFamily(new HColumnDescriptor("info"));//120秒

            admin.createTable(desc);

            //预分区
           /* String startKey = "rowKey-001";
            String endKey = "rowKey-099";
            int numRegins = 5;
            admin.createTable(desc, Bytes.toBytes(startKey),Bytes.toBytes(endKey), numRegins);*/
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
            admin.disableTable(TableName.valueOf("dtinone"));
            admin.deleteTable(TableName.valueOf("dtinone"));

            admin.disableTables("dtinone.*");
            admin.deleteTables("dtinone.*");
        }
    }
    public static void main(String[] args) throws Exception {
        Dtinone_Ddl dtinoneDel= new Dtinone_Ddl();
        Connection connect = dtinoneDel.connectToHbase(hbaseMasterIPS, hbaseMasterPort, hbaseZnode);
//        dtinoneDel.listTables(connect.getAdmin());
        dtinoneDel.createTable(connect.getAdmin());
//        dtinoneDel.disableTable(connect.getAdmin());
//        dtinoneDel.enableTable(connect.getAdmin());
//        dtinoneDel.modifyTable(connect.getAdmin());
//        dtinoneDel.descriptor(connect.getAdmin());
//        dtinoneDel.deleteTable(connect.getAdmin());
    }
}
