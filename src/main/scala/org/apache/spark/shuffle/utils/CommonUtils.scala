package org.apache.spark.shuffle.utils

import org.apache.spark.internal.Logging
import scala.concurrent.duration._


import java.io.File;
import org.ini4j.Ini


object CommonUtils extends Logging {
    
    private class CommonUtilsTimeoutException(s:String) extends Exception(s){}

    /**
      * Perform polling up to timeLimit
      *
      * @return
      */
    def safePolling(poll: () => Unit, pollCond: () => Boolean = () => {false}, timeLimit: Long = 10000,
      eMsg: String = "Got timeout when polling", sleep: Long = 0) {
      val deadline = timeLimit.millis.fromNow

      while (pollCond()) {
        if (deadline.isOverdue()) {
          if (eMsg != null) {
            throw new CommonUtilsTimeoutException(eMsg)
          }
        }

        poll()
        if (sleep > 0) {
          Thread.sleep(sleep)
        }
      }
    }
}