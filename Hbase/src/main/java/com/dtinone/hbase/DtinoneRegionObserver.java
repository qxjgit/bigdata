package com.dtinone.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/*
*hbase协处理器
*/
public class DtinoneRegionObserver extends BaseRegionObserver{

    private static final Log log = LogFactory.getLog(DtinoneRegionObserver.class);

    private Connection connection = null;

    private final String LOG_TABLE = "default:logs";

    private final String FAMILY = "info";

    private final String COLUMN_TABLE = "table";

    private final String COLUMN_TYPE = "type";

    private final String COLUMN_TIME = "time";

    public void init(String zkIps, int zkPort, String znode) throws Exception {
        if (!"".equals(zkIps)) {
            Configuration that = new Configuration();
            that.set("hbase.rpc.protection","privacy");
            that.set("hbase.zookeeper.quorum", zkIps);
            that.set("hbase.zookeeper.property.clientPort", String.valueOf(zkPort));
            that.set("zookeeper.znode.parent",znode);
            that.set("hbase.rpc.timeout","3000");
            that.set("zookeeper.session.timeout","3000");
//            that.set("hadoop.home.dir","/usr/hdp/2.6.0.3-8/hadoop");
            that.set("hadoop.home.dir","/usr/local/hadoop");
            Configuration conf = HBaseConfiguration.create(that);
            this.connection = ConnectionFactory.createConnection(conf);
        }
        else{
            throw new Exception("IP address is not set");
        }
    }

    private void log(ObserverContext<RegionCoprocessorEnvironment> e, String type) throws IOException{
        RegionCoprocessorEnvironment env = e.getEnvironment();
        TableName tableName = env.getRegion().getTableDesc().getTableName();
        if(!"hbase".equalsIgnoreCase(tableName.getNameAsString()) &&
                !LOG_TABLE.equalsIgnoreCase(tableName.getNameWithNamespaceInclAsString())){
            long regionId = env.getRegion().getRegionInfo().getRegionId();
            if("P".equals(type)){
                log.info("region-" + regionId + "DtinoneRegionObserver.postPut()............");
            }else if("D".equals(type)){
                log.info("region-" + regionId + "DtinoneRegionObserver.postDelete()............");
            }
            String table = tableName.getNameAsString();
            long time = System.currentTimeMillis();
            String rowkey = table + "|" + type + "|" + (-time);
            Put logPut = new Put(Bytes.toBytes(rowkey));
            logPut.addColumn(Bytes.toBytes(FAMILY),Bytes.toBytes(COLUMN_TABLE),Bytes.toBytes(table));
            logPut.addColumn(Bytes.toBytes(FAMILY),Bytes.toBytes(COLUMN_TYPE),Bytes.toBytes(type));
            logPut.addColumn(Bytes.toBytes(FAMILY),Bytes.toBytes(COLUMN_TIME),Bytes.toBytes(time));
            Table logTable = null;
            try {
                logTable = connection.getTable(TableName.valueOf(LOG_TABLE));
                logTable.put(logPut);
            } finally {
                if(logTable != null){
                    logTable.close();
                }
            }
        }
    }
    @Override
    public void start(CoprocessorEnvironment env) throws IOException{
        log.info("DtioneReginObserver.start()..............");
        if(env instanceof RegionCoprocessorEnvironment){
            try {
                this.init("dtinone",2181, "/hbase-unsecure");
            }catch (Exception e){
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }else{
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException{
        log.info("DtinoneRegionObserver.stop()............");
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException{
        this.log(e, "P");
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,Durability durability) throws IOException{
        this.log(e,"D");
    }
}
