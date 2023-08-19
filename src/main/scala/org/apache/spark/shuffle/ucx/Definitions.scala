package org.apache.spark.shuffle.ucx.utils

object UcpSparkAmId {
    val InitExecutorReq = 0;
    val InitExecutorAck = 1;
    val MapperInfo = 2;
    val FetchBlockReq = 3;
    val FetchBlockReqAck = 4;
}
