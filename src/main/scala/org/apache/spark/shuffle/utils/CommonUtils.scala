package org.apache.spark.shuffle.utils

import org.apache.spark.internal.Logging
import scala.concurrent.duration._


import java.io.File;
import org.ini4j.Ini


object CommonUtils extends Logging {
    
    private class CommonUtilsTimeoutException(s:String) extends Exception(s){}

    /**
      * Perform polling up to deadline with sleep in between
      *
      * @return
      */
    def safePolling(pollFunc: () => Unit, pollCondition: () => Boolean = () => {false},
      deadline: Deadline = 10.seconds.fromNow,
      eMsg: String = "Got timeout when polling", sleep: Duration = Duration(0, "millis")) {

      while (pollCondition()) {
        if (deadline.isOverdue()) {
          if (eMsg != null) {
            throw new CommonUtilsTimeoutException(eMsg)
          }
        }

        pollFunc()
        if (sleep.toMillis > 0) {
          Thread.sleep(sleep.toMillis)
        }
      }
    }
}