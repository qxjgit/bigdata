package com.c503.sc.qiuxj;

import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 *
 */
public class Dtinone_DistributedFileSystem extends Dtinone_Hdfs{
    public static void main(String[] args) throws Exception{
        Dtinone_Configuration dtinoneFileSystem = new Dtinone_Configuration();
        DistributedFileSystem hdfs = (DistributedFileSystem)dtinoneFileSystem.connectToHdfs(Dtinone_Hdfs.hdfsMasterIPAndPort);
//        dtinoneFileSystem.addConf(hdfs);
        //dtinoneFileSystem.getServerDefaults(hdfs);//org.apache.hadoop.fs.FsServerDefaults@3eb77ea8
//        dtinoneFileSystem.getBlockSummary(hdfs);
//        dtinoneFileSystem.getCapacityStatus(hdfs);
        dtinoneFileSystem.getDataNodes(hdfs);
    }
}
