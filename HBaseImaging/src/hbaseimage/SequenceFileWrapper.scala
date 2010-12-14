package hbaseimage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import org.apache.hadoop.io.{Writable, SequenceFile}

/**
 * Created by IntelliJ IDEA.
 * User: FaKod
 * Date: Oct 24, 2010
 * Time: 6:12:37 AM
 * To change this template use File | Settings | File Templates.
 */

trait SequenceFileWrapper {
  /**
   * Hadoop configuration instance
   */
  val conf: Configuration

  /**
   * apply the Writer, Key and Value instance for function f
   */
  def doAppend[K: ClassManifest, V: ClassManifest](path: String, filename: String)
                                               (f: (SequenceFile.Writer, K, V) => Unit): Unit = {
    val k = implicitly[ClassManifest[K]]
    val v = implicitly[ClassManifest[V]]

    val fileSystem = FileSystem.get(URI.create(path), conf)
    val p = new Path(path, filename)

    val key = k.erasure.newInstance
    val value = v.erasure.newInstance
    val sf = SequenceFile.createWriter(fileSystem, conf, p, k.erasure, v.erasure, SequenceFile.CompressionType.NONE)

    f(sf, key.asInstanceOf[K], value.asInstanceOf[V])

    sf.close
  }

  /**
   * simple append with += replacement 
   */
  implicit def toRichWriter(sfw: SequenceFile.Writer) = new {
    def +=(k: Writable, v: Writable) = sfw.append(k, v)
  }
}