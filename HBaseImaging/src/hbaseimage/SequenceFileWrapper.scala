package hbaseimage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import org.apache.hadoop.io.{Writable, SequenceFile}

/**
 * @author Christopher Schmidt
 *
 * this is for easier sequence file handling
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