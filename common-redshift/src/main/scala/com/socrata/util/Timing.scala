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

  def TimedResult[T](block: => T)(fun: (T, Duration) => Unit): T = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    fun(result, Duration(end - start, MILLISECONDS))
    result
  }

  def TimedResultReturning[T,P](block: => T)(fun: (T, Duration) => P): P = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    fun(result, Duration(end - start, MILLISECONDS))
  }

  def TimedResultReturningTransformed[T, P](block: => T)(fun: (T, Duration) => P): P = {
    val start = System.currentTimeMillis()
    val result = block
    val end = System.currentTimeMillis()
    fun(result, Duration(end - start, MILLISECONDS))
  }

}
