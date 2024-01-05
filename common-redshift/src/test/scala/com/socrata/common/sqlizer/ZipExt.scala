package com.socrata.common.sqlizer

import org.junit.jupiter.api.Assertions._

object ZipExt {
  implicit class ZipUtils[T](seq: Seq[T]) {
    def zipExact[R](other: Seq[R]): Seq[(T, R)] = {
      if (seq.length == other.length) {
        seq.zip(other)
      } else fail(s"""You cannot pass...${seq} and ${other} are different lengths
The dark fire will not avail you, flame of Udûn. """)
    }
  }
}
