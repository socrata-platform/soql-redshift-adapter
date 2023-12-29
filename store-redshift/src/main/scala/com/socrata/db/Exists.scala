package com.socrata.db

object Exists {
  trait Exists[A]
  case class Does[A](a: A) extends Exists[A]
  case class DoesNot[A](a: A) extends Exists[A]
}
