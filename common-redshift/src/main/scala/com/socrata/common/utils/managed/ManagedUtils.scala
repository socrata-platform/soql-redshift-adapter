package com.socrata.common.utils.managed

import com.rojoma.simplearm.v2.Managed

object ManagedUtils {
  def construct[T](t: T): Managed[T] = new Managed[T] {
    def run[B](f: T => B): B = f(t)
  }
}
