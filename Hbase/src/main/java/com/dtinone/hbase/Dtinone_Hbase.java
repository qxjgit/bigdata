package com.dtinone.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

/**
 * Hbase 集群管理基类
 * @author dtinone
 * @copyright 加米谷大数据学院
 * @webSite http://www.dtinone.com/
 * @license 加米谷版权所有，仅供加米谷大数据学院内部使用，禁止其他机构使用，违者必究，追究法律责任。
 */
public class Dtinone_Hbase {

    public final static String hbaseMasterIPS = "dtinone";
    public final static int hbaseMasterPort = 2181;
    public final static String hbaseZnode= "/hbase-unsecure";
    public final static int defaultVersion = 3;
    public final static String tableName = "dtinone";

    private Configuration conf;

    /**
     * 连接到Hbase
     * @param zkIps Hbase所依赖的Zookeeper集群的IP
     * @param zkPort Zookeeper集群的IP
     * @throws IOException IO访问异常
     * @author dtinone--加米谷大数据学院
     */
    public Connection connectToHbase(ArrayList<String> zkIps, int zkPort, String znode) throws IOException {
        String zkIp = "";
        if (zkIps != null && zkIps.size() > 0) {
            for (String ip : zkIps) {
                zkIp = zkIp + ip + ",";
            }
            if (zkIp.endsWith(",")) {
                zkIp = zkIp.substring(0, zkIp.length() - 1);
            }
        }
        return this.connectToHbase(zkIp, zkPort, znode);
    }

    /**
     * 连接到Hbase
     * @param zkIps Hbase所依赖的Zookeeper集群的IP
     * @param zkPort Zookeeper集群的IP
     * @throws IOException IO访问异常
     * @author dtinone--加米谷大数据学院
     */
    public Connection connectToHbase(String zkIps, int zkPort, String znode) throws IOException {
        Connection connect = null;
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
            this.conf = HBaseConfiguration.create(that);
            connect = ConnectionFactory.createConnection(conf);
        }
        return connect;
    }

    /**
     * 获取Hbase集群配置
     * @return Hbase集群配置
     * @author dtinone--加米谷大数据学院
     */
    public Configuration getConf() {
        return conf;
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
