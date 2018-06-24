package com.c503.sc.qiuxj;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;

import java.io.IOException;

/**
 * hdfs 集群管理基类
 * @author dtinone
 * @copyright 加米谷大数据学院
 * @webSite http://www.dtinone.com/
 * @license 加米谷版权所有，仅供加米谷大数据学院内部使用，禁止其他机构使用，违者必究，追究法律责任。
 */
public class Dtinone_Hdfs {

    public final static String basePath = "E:/dtinone/testData";

    public static final String hdfsMasterIPAndPort = "hdfs://dtinone:8020";

    public final static String hdfsMasterIPAndPortHA = "master.dtinone:8020,slave1.dtinone:8020";

    public final static String nameService = "dtinonens";

    private Configuration conf;

    /**
     * 连接到Hdfs集群，单Master
     *
     * @param hdfsMasterIPAndPort Hdfs的Master的地址与端口，格式：ip:port
     * @throws IOException
     * @throws Dtinone_NoIPException
     * @author dtinone--加米谷大数据学院
     */
    public FileSystem connectToHdfs(String hdfsMasterIPAndPort) throws IOException, Dtinone_NoIPException {
        FileSystem hdfs = null;
        if (hdfsMasterIPAndPort == null || "".equals(hdfsMasterIPAndPort) || hdfsMasterIPAndPort.indexOf(":") < 0) {
            throw new Dtinone_NoIPException("Please specify IP address.");
        }
        String[] temp = hdfsMasterIPAndPort.split(":");
        if (temp.length >= 2) {
            this.conf = new Configuration();
            conf.set("fs.defaultFS", hdfsMasterIPAndPort);
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            hdfs = FileSystem.get(conf);
        }
        return hdfs;
    }

    /**
     * 连接到Hdfs集群，多Master，高可用
     *
     * @param hdfsMasterIPAndPortHA Hdfs的Master的地址与端口，格式：ip1:port,ip2:port
     * @param nameService
     * @throws IOException
     * @throws Dtinone_NoIPException
     * @author dtinone--加米谷大数据学院
     */
    public FileSystem connectToHdfs(String hdfsMasterIPAndPortHA, String nameService) throws IOException, Dtinone_NoIPException {
        FileSystem hdfs = null;
        if (hdfsMasterIPAndPortHA == null || "".equals(hdfsMasterIPAndPortHA) ||
                hdfsMasterIPAndPortHA.indexOf(",") < 0 || hdfsMasterIPAndPortHA.indexOf(":") < 0) {
            throw new Dtinone_NoIPException("Please specify IP address.");
        }
        this.conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + nameService);
        conf.set("dfs.nameservices", nameService);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        int nnIndex = 0;
        String nnStr = "";
        String[] ipAndPorts = hdfsMasterIPAndPortHA.split(",");
        boolean allowInitFlag = false;
        for (String ipAndPort : ipAndPorts) {
            String[] temp = ipAndPort.split(":");
            if (temp.length >= 2) {
                allowInitFlag = true;
                String ip = temp[0];
                int port = Integer.valueOf(temp[1]);
                String nn = "nn" + ++nnIndex;
                conf.set("dfs.namenode.rpc-address." + nameService + "." + nn, ip + ":" + port);
                nnStr += nn + ",";
            }
        }
        if (allowInitFlag) {
            conf.set("dfs.ha.namenodes." + nameService, nnStr.substring(0, nnStr.length() - 1));
            conf.set("dfs.client.failover.proxy.provider." + nameService, ConfiguredFailoverProxyProvider.class.getName());
            hdfs = FileSystem.get(conf);
        }
        return hdfs;
    }

    /**
     * 获取hdfs集群配置
     *
     * @param
     * @return hdfs集群配置
     * @author dtinone--加米谷大数据学院
     */
    public Configuration getConf() {
        return conf;
    }

    /**
     * 集群中默认配置
     * @param hdfs
     * @return
     * @throws Exception
     */
    public void getConf(FileSystem hdfs) throws Exception {
        Configuration conf = hdfs.getConf();
        System.out.println(conf.get("fs.default.name"));
        System.out.println(conf.get("fs.defaultFS"));
        System.out.println(conf.get("dfs.nameservices"));
        System.out.println(conf.get("fs.hdfs.impl"));
        System.out.println(conf.get("fs.file.impl"));
    }

    /**
     * 集群中默认配置
     * @param hdfs
     * @throws Exception
     */
    public void addConf(FileSystem hdfs) throws Exception {
        Configuration conf = new Configuration();
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
        System.out.println(conf.get("fs.default.name"));
        System.out.println(conf.get("fs.defaultFS"));
        System.out.println(conf.get("dfs.nameservices"));
        System.out.println(conf.get("fs.hdfs.impl"));
        System.out.println(conf.get("fs.file.impl"));
    }

    /**
     * 集群中默认配置
     * @param hdfs
     * @throws Exception
     */
    public void getServerDefaults(DistributedFileSystem hdfs) throws Exception{
        FsServerDefaults serverDefaults = hdfs.getServerDefaults();
        System.out.println(serverDefaults);
    }

    /**
     * 获取hdfs集群块的概要信息
     * @param hdfs
     * @throws Exception
     */
    public void getBlockSummary(DistributedFileSystem hdfs) throws Exception{
        //获取块大小(128M)单位是字节
        System.out.println("默认块大小字节："+hdfs.getDefaultBlockSize());

        //获取复制集数
        System.out.println("默认复制集数："+hdfs.getDefaultReplication());

        //集群坏块数量
        System.out.println("集群坏块数量："+hdfs.getCorruptBlocksCount());

        //集群丢失块数量
        System.out.println("集群丢失块数量："+hdfs.getMissingBlocksCount());

        //集群低于副本数块数量
        System.out.println("集群低于副本数块数量："+hdfs.getUnderReplicatedBlocksCount());

        //集群磁盘使用
        System.out.println("集群磁盘使用："+hdfs.getUsed());
    }

    /**
     * 在hdfs集群中，集群状态
     * @param hdfs
     * @throws Exception
     */
    public void getCapacityStatus(DistributedFileSystem hdfs) throws Exception{
        FsStatus status = (hdfs.getStatus());
        System.out.println("Cluster Capacity: " + status.getCapacity());
        System.out.println("Cluster DFS Used: " + status.getCapacity());
        System.out.println("Cluster Non DFS Used: " + (status.getCapacity() - status.getUsed() - status.getRemaining()));
        System.out.println("Cluster Remaining: " + status.getRemaining());
    }

    /**
     * 在hdfs集群中，获取所有DataNode节点信息
     * @param hdfs 集群
     * @throws Exception
     */
    public void getDataNodes(DistributedFileSystem hdfs) throws Exception{
        DatanodeInfo[] datanodeInfos = hdfs.getDataNodeStats();
        for(DatanodeInfo datanodeInfo:datanodeInfos){
            System.out.println("NodeName="+datanodeInfo.getName());
            System.out.println("HostName="+datanodeInfo.getHostName());
            System.out.println("Capacity="+datanodeInfo.getCapacity());
            System.out.println("DfsUsed="+datanodeInfo.getDfsUsed());
            System.out.println("NonDfsUsed="+datanodeInfo.getNonDfsUsed());
            System.out.println();
        }
    }
}
