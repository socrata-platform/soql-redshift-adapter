package com.socrata.common.db

object Exists {
  trait Exists[A]
  case class Updated[A](a: A) extends Exists[A]
  case class Inserted[A](a: A) extends Exists[A]
}
