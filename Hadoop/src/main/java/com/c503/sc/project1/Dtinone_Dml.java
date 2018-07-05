package com.c503.sc.project1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class Dtinone_Dml extends Dtinone_Hbase{
    private void put(Connection connect) throws Exception{
        if (connect != null) {
//            Table table = connect.getTable(TableName.valueOf("dtinone"));
            Table table = connect.getTable(TableName.valueOf("logs"));
            try {
                Put put = new Put(Bytes.toBytes("rowKey-001"));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("id"),objectToByte("0001"));
//                put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("id"),objectToByte("0001"));
//                put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("name"),objectToByte("dtinone"));
//                put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("size"),objectToByte("10000"));
//                put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("datetime"),objectToByte("20171010"));
//                put.addColumn(Bytes.toBytes("cfName"),Bytes.toBytes("sex"),objectToByte("男"));
//                put.addColumn(Bytes.toBytes("cfName"),Bytes.toBytes("age"),objectToByte("30"));
                table.put(put);
            }
            finally {
                table.close();
            }
        }
    }

    private void modify(Connection connect) throws Exception{
        if (connect != null) {
            Table table = connect.getTable(TableName.valueOf("dtinone"));
            try {
                Put put = new Put(Bytes.toBytes("rowKey-001"));
                put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("id"),objectToByte("0001-m"));
                put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("name"),objectToByte("dtinone-m"));
                table.put(put);
            }
            finally {
                table.close();
            }
        }
    }
    private void get(Connection connect) throws Exception{
        if (connect != null) {
            Table table = connect.getTable(TableName.valueOf("dtinone"));
            try {
                Get get = new Get(Bytes.toBytes("rowKey-001"));
//                get.addFamily(Bytes.toBytes("cfn"));
//                get.addColumn(Bytes.toBytes("cfn"), Bytes.toBytes("name"));
//                get.setTimeStamp(1517324688030L);
//                get.setTimeRange(1517324688030L, 1517324688030L);
//                get.setMaxVersions(3);
                Result r = table.get(get);
                printResult(r);
            }
            finally {
                table.close();
            }
        }
    }

    private void delete(Connection connect) throws Exception{
        if (connect != null) {
            Table table = connect.getTable(TableName.valueOf("dtinone"));
            try {
                Delete delete = new Delete(Bytes.toBytes("rowKey-001"));
                delete.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("datetime"));
//                delete.addFamily(Bytes.toBytes("cfn"));
//                delete.addFamily(Bytes.toBytes("cfName"));
                table.delete(delete);
            }
            finally {
                table.close();
            }
        }
    }
    private void puts(Connection connect) throws Exception{
        if (connect != null) {
            Table table = connect.getTable(TableName.valueOf("dtinone"));
            try {
                List<Put> puts = new ArrayList();
                for (int i = 1;i < 100; i++){
                    String rowkey = "rowKey-";
                    if(i < 10) rowkey += "00" + i;
                    else if(i >=10 && i < 100) rowkey += "0" + i;
                    else rowkey += i;
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("id"),objectToByte(""+i));
                    put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("name"),objectToByte("dtinone"+i));
                    put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("size"),objectToByte("10000"));
                    put.addColumn(Bytes.toBytes("cfn"),Bytes.toBytes("datetime"),objectToByte("20171010"));
                    put.addColumn(Bytes.toBytes("cfName"),Bytes.toBytes("sex"),objectToByte("男"));
                    put.addColumn(Bytes.toBytes("cfName"),Bytes.toBytes("age"),objectToByte("30"));
                    puts.add(put);
                }
                table.put(puts);
            }
            finally {
                table.close();
            }
        }
    }
    private void scan(Connection connect) throws Exception{
        if (connect != null) {
            Table table = connect.getTable(TableName.valueOf("dtinone"));
            try {
                String startRow = "rowKey-015";
                String stopRow = "rowKey-050";
                String colFamily = "cfn";
                String colQualifier = "name";
                int scanMaxVersions = 3;
                Scan scan = new Scan();
//                scan.setStopRow(Bytes.toBytes(stopRow));
//                Scan scan = new Scan(Bytes.toBytes(startRow));
//                Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
//                scan.addFamily(Bytes.toBytes(colFamily));
//                scan.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(colQualifier));
//                scan.setTimeStamp(1517324588031L);
//                scan.setTimeRange(1517324588031L,1517324688031L);//1000000 = 1s
               /* Scan scan = new Scan(Bytes.toBytes(stopRow),Bytes.toBytes(startRow));
                scan.setReversed(true);*/
//                scan.setMaxVersions(scanMaxVersions);
                ResultScanner rs = table.getScanner(scan);
                for(Result r = rs.next();r != null; r = rs.next()){
                    this.printResult(r);
                }
            }
            finally {
                table.close();
            }
        }
    }
    private void printResult(Result r){
        List<Cell> cellList = r.listCells();
        if(cellList != null && !cellList.isEmpty()){
            for(Cell cell : cellList){
                System.out.println(Bytes.toString(r.getRow()) +
                "-" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                ":" + cell.getTimestamp() +
                "=" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
    public static void main(String[] args) throws Exception{
        Dtinone_Dml dtinoneDml = new Dtinone_Dml();
        Connection connect = dtinoneDml.connectToHbase(hbaseMasterIPS,hbaseMasterPort,hbaseZnode);
        dtinoneDml.put(connect);
//        dtinoneDml.modify(connect);
//        dtinoneDml.get(connect);
//        dtinoneDml.delete(connect);
//        dtinoneDml.puts(connect);
//        dtinoneDml.scan(connect);
    }
}
