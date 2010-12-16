package hbaseimage

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import java.io.FileInputStream
import javax.imageio.{ImageReader, ImageIO}
import org.apache.hadoop.hbase.client.{Put, HTable}


import Conversions._

/**
 * @author Christoher Schmidt
 *
 * Test Class for writing an image in a HTable
 */
object HBaseTest extends HBaseWrapper {

  /**
   * main method. No paramter
   */
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