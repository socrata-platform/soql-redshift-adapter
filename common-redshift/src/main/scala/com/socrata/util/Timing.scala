package com.socrata.util

import scala.concurrent.duration.{Duration, MILLISECONDS}

object Timing {

  def Timed[T](block: => T)(fun: Duration => Unit): T = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    fun(Duration(end - start, MILLISECONDS))
    result
  }

}
