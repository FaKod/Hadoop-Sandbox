package hbaseimage

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import javax.imageio.ImageIO
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.awt.color.ColorSpace
import org.apache.hadoop.io.{Text, LongWritable, BytesWritable}
import java.awt.image.{BufferedImage, ColorConvertOp}
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration

/**
 * @author Christopher Schmidt
 *
 * Hadoop Mapper Class
 *
 * uses HBase Table created with statement
 * "create 'images', {NAME => 'image', VERSIONS => 1},
 * {NAME => 'info', VERSIONS => 1}, {NAME => 'loc', VERSIONS => 1}, {NAME => 'loc', VERSIONS => 1}"
 */

class GrayScaleMapper extends Mapper[Text, BytesWritable, Text, BytesWritable] with HBaseWrapper {

  /**
   * getting the link to HTable images
   */
  val conf = HBaseConfiguration.create
  val hTable = new HTable(conf, "images")

  /**
   * type alias for simplicity
   */
  type ContextTypeAlias = Mapper[Text, BytesWritable, Text, BytesWritable]#Context

  /**
   * main map method.
   * @param key the tile number
   * @param file the raw image data
   */
  override def map(key: Text, file: BytesWritable, context: ContextTypeAlias) = {

    val split = context.getInputSplit.asInstanceOf[FileSplit]
    println("got split from file: " + split.getPath + " for key: " + key)

    /**
     * convert byte array to BufferedImage
     */
    import Conversions._
    val intArray:Array[Int] = file.getBytes
    println("available Ints: + " + intArray.size)

    /**
     * get size of image from Key text
     */
    val ds = key.toString.split(" ")
    val (width, height) = (ds(ds.size-2).toInt, ds(ds.size-1).toInt)
    println("Image with Width: " + width + " and Hight: " + height)

    /**
     * create BufferedImage from Integer Array
     */
    val buffImg = new BufferedImage (width, height, BufferedImage.TYPE_BYTE_GRAY)
    buffImg.getRaster.setPixel(0, 0, intArray)

    /**
     * grayscale the image's pixel
     */
    val op = new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY), null)
    val grayImage = op.filter(buffImg, null)

    /**
     * convert BufferedImage to byte array
     */
    val baos = new ByteArrayOutputStream

    ImageIO.write(grayImage, "tif", baos)
    val imageAsByteArray = baos.toByteArray

    // normally we use this here
    // context.write(key, new BytesWritable(imageAsByteArray))
    // can be omitted since we write directly into HBase here

    /**
     * writing the image into HBase
     */
    println("putting image with key: " + key.toString)
    doPut(hTable, key.toString) {
      p =>

        p += ("image", "standard", imageAsByteArray)

        p += ("info", "name", key.toString);
        p += ("info", "description", "Image Description from Image Matadata")

        p += ("loc", "lat", "Location... Latitude")
        p += ("loc", "lon", "Location... Longitude")
        p += ("loc", "extend", "Zoom... Extend")
    }
  }
}