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
 * Created by IntelliJ IDEA.
 * User: Christopher Schmidt
 * Date: Oct 24, 2010
 * Time: 6:34:07 AM
 *
 * uses HBase Table created with statement
 * "create 'images', {NAME => 'image', VERSIONS => 1},
 * {NAME => 'info', VERSIONS => 1}, {NAME => 'loc', VERSIONS => 1}, {NAME => 'loc', VERSIONS => 1}"
 */

class GrayScaleMapper extends Mapper[Text, BytesWritable, Text, BytesWritable] with HBaseWrapper {

  val conf = HBaseConfiguration.create
  val hTable = new HTable(conf, "images")

  /**
   *
   */
  override def map(key: Text, file: BytesWritable,
                   context: Mapper[Text, BytesWritable, Text, BytesWritable]#Context) = {

    val split = context.getInputSplit.asInstanceOf[FileSplit]
    println("got split from file: " + split.getPath + " for key: " + key)

    // convert byte array to BufferedImage
    import Conversions._
    val intArray:Array[Int] = file.getBytes
    println("available Ints: + " + intArray.size)

    // get size of image from Key text
    val ds = key.toString.split(" ")
    val (width, height) = (ds(ds.size-2).toInt, ds(ds.size-1).toInt)
    println("Image with Width: " + width + " and Hight: " + height)

    // create BufferedImage from Integer Array
    val buffImg = new BufferedImage (width, height, BufferedImage.TYPE_BYTE_GRAY)
    buffImg.getRaster.setPixel(0, 0, intArray)

    // grayscale the image's pixel
    val op = new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY), null)
    val grayImage = op.filter(buffImg, null)

    // convert BufferedImage to byte array
    val baos = new ByteArrayOutputStream

    ImageIO.write(grayImage, "tif", baos)
    val imageAsByteArray = baos.toByteArray

    //context.write(key, new BytesWritable(imageAsByteArray))

    println("putting image with key: " + key.toString)
    doPut(hTable, key.toString) {
      p =>

        //p += ("image", "standard", imageAsByteArray)

        p += ("info", "name", key.toString);
        p += ("info", "description", "Image Description from Image Matadata")

        p += ("loc", "lat", "Location... Latitude")
        p += ("loc", "lon", "Location... Longitude")
        p += ("loc", "extend", "Zoom... Extend")
    }
  }
}