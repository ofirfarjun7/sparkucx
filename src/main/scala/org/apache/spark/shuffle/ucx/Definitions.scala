package org.apache.spark.shuffle.ucx.utils

// object Definitions {

// // enum UcpSparkAmId extends Int {
// //     InitExecutorReq = 0,
// //     InitExecutorAck,
// //     FetchReq,
// //     FetchAck
// // }

// enum UcpSparkAmId extends Int {
//     InitExecutorReq = 0,
//     InitExecutorAck,
//     FetchReq,
//     FetchAck



// }

object UcpSparkAmId {
    // val InitExecutorReq = 0;
    val InitExecutorReq = 0;
    val InitExecutorAck = 1;
    val MapperInfo = 2;
    val FetchBlockReq = 3;
    val FetchBlockReqAck = 4;
}