package com.c503.sc.qiuxj;

import org.apache.hadoop.fs.FileSystem;

public class Dtinone_Configuration extends Dtinone_Hdfs {

    public static void main(String[] args) throws Exception {
        Dtinone_Configuration dtinoneFileSystem = new Dtinone_Configuration();
        FileSystem hdfs = dtinoneFileSystem.connectToHdfs(Dtinone_Hdfs.hdfsMasterIPAndPort);
//        dtinoneFileSystem.addConf(hdfs);
        dtinoneFileSystem.getConf(hdfs);
    }
}
