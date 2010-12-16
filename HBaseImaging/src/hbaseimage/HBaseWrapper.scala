package hbaseimage

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{HTable, Put}

/**
 * @author Christopher Schmidt
 *
 * This is a "put" wrapper trait for HBase.
 * To do it more the Scala way
 */

trait HBaseWrapper {

  /**
   * provide a doPut context. Use += afterwards
   *
   * @param table HTable instance for this context
   * @param rowKey String to define the row
   * @param f function where instance of Putter can be applied
   */
   def doPut(table: HTable, rowKey: String)(f: Putter => Unit) = {
    val rk = Bytes.toBytes(rowKey)
    val put = new Put(Bytes.toBytes(rowKey))
    val putter = new Putter(put)
    f(putter)
    table.put(put)
  }

  /**
   * provides method +=
   */
  class Putter(p: Put) {

     /**
     * puts the given parameter to a HTable row
     *
     * @param v triple of column family, column qualifier and value
     */
    def +=(v: (Array[scala.Byte], Array[scala.Byte], Array[scala.Byte])) = p.add(v._1, v._2, v._3)
  }
}