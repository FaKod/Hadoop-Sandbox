package hbaseimage

import org.apache.hadoop.hbase.util.Bytes

/**
 * @author Christopher Schmidt
 *
 * This is a set of conversion methods to create byte or Int arrays
 */

object Conversions {

  /**
   * Byte array to Int
   */
  implicit def byte2Integer(v: Array[scala.Byte]): Int = Bytes.toInt(v)

  /**
   * Int to Byte array
   */
  implicit def integer2Byte(v: Int): Array[scala.Byte] = {
    var value = v
    val b = new Array[scala.Byte](4)
    for (i <- 3 to (1, -1)) {
      b(i) = value.byteValue
      value = value >>> 8
    }
    b(0) = value.byteValue
    b
  }

  /**
   * Int array to Byte array
   */
  implicit def intArrayToByteArray(a: Array[Int]): Array[scala.Byte] = {
    val byteArray = new Array[scala.Byte](a.size * 4)
    var t = 0
    for (i <- 0 until a.size) {
      val b: Array[scala.Byte] = a(i)
      byteArray(t) = b(0)
      byteArray(t + 1) = b(1)
      byteArray(t + 2) = b(2)
      byteArray(t + 3) = b(3)
      t = t + 4
    }
    byteArray
  }

  /**
   * Byte array to Int array
   */
  implicit def byteArrayToIntArray(v: Array[scala.Byte]):Array[Int] = {
    val intArray = new Array[Int](v.size / 4)
    var t = 0
    for(i <- 0 until (v.size, 4)) {
      intArray(t) = Bytes.toInt(v, i)
      t = t + 1
    }
    intArray
  }

  /**
   * String to Byte array
   */
  implicit def stringToByteArray(s: String): Array[scala.Byte] = {
    Bytes.toBytes(s)
  }
}