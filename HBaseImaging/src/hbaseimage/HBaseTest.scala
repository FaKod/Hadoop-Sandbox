package hbaseimage

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import java.io.FileInputStream
import javax.imageio.{ImageReader, ImageIO}
import org.apache.hadoop.hbase.client.{Put, HTable}

/**
 * Created by IntelliJ IDEA.
 * User: training
 * Date: Oct 16, 2010
 * Time: 6:11:16 AM
 * To change this template use File | Settings | File Templates.
 */

object Conversions {
  implicit def byte2Integer(v: Array[scala.Byte]): Int = Bytes.toInt(v)

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

  implicit def byteArrayToIntArray(v: Array[scala.Byte]):Array[Int] = {
    val intArray = new Array[Int](v.size / 4)
    var t = 0
    for(i <- 0 until (v.size, 4)) {
      intArray(t) = Bytes.toInt(v, i)
      t = t + 1
    }
    intArray
  }

  implicit def stringToByteArray(s: String): Array[scala.Byte] = {
    Bytes.toBytes(s)
  }
}

import Conversions._

object HBaseTest extends HBaseWrapper {
  def main(arg: Array[String]): Unit = {
    val conf = HBaseConfiguration.create
    val hTable = new HTable(conf, "images")

    val imagePath = "/home/training/ws_copy/image_in.gif"

    val imgReader = ImageIO.getImageReadersByFormatName("gif").next.asInstanceOf[ImageReader]
    val iis = ImageIO.createImageInputStream(new FileInputStream(imagePath))
    imgReader.setInput(iis, true)

    val bufferedImage = imgReader.read(0)
    val tileArray = new Array[Int](imgReader.getWidth(0) * imgReader.getHeight(0))

    //val md = imgReader.getImageMetadata(0)
    val imageArray = bufferedImage.getRGB(0, 0, imgReader.getWidth(0), imgReader.getHeight(0), tileArray, 0, 0)

    doPut(hTable, imagePath) {
      p =>
        val name = imagePath.substring(imagePath.lastIndexOf("/"))

        p += ("image", "standard", imageArray)
        //p.add("image", "-8", imageArray)
        //p.add("image:","-16", imageArray)
        //p.add("image:","-32", imageArray)

        p += ("info", "name", name);
        p += ("info", "description", "Image Description from Image Matadata")

        p += ("loc", "lat", "Location... Latitude")
        p += ("loc", "lon", "Location... Longitude")
        p += ("loc", "extend", "Zoom... Extend")
    }
  }
}