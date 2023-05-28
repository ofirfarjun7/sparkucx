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
    val InitExecutorReq = 0;
    val PreInitExecutorReq = 2;
    val InitExecutorAck = 1;
    val FetchReq = 2;
    val FetchAck = 3;
}