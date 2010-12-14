package hbaseimage

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{HTable, Put}

/**
 * Created by IntelliJ IDEA.
 * User: Christopher Schmidt
 * Date: Oct 25, 2010
 * Time: 9:12:50 AM
 */

trait HBaseWrapper {

  /**
   *
   */
   def doPut(table: HTable, rowKey: String)(f: Putter => Unit) = {
    val rk = Bytes.toBytes(rowKey)
    val put = new Put(Bytes.toBytes(rowKey))
    val putter = new Putter(put)
    f(putter)
    table.put(put)
  }

  /**
   *
   */
  class Putter(p: Put) {
    def +=(v: (Array[scala.Byte], Array[scala.Byte], Array[scala.Byte])) = p.add(v._1, v._2, v._3)
  }
}