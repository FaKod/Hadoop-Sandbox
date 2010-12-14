package hbaseimage

import java.io.FileInputStream
import java.awt.Rectangle
import org.apache.hadoop.conf.Configuration
import javax.imageio.{ImageReader, ImageIO}
import org.apache.hadoop.io.{BytesWritable, Text}

/*
 *
 */
object Start {
  def main(argv: Array[String]): Unit = {

    val filename = argv(0)
    val filenameParts = filename split ('.')
    val filenameSuffix = filenameParts(filenameParts.length - 1)

    val imgReader = ImageIO.getImageReadersByFormatName(filenameSuffix).next.asInstanceOf[ImageReader]
    val iis = ImageIO.createImageInputStream(new FileInputStream(filename))
    imgReader.setInput(iis, true)

    val splitter = new ImageSplitter(imgReader)
  }
}

/**
 * 
 */
class ImageSplitter(imageReader: ImageReader) extends SequenceFileWrapper {
  val conf = new Configuration

  val imageIndex = 0
  val imageWidth = imageReader getWidth (imageIndex)
  val imageHeight = imageReader getHeight (imageIndex)
  val readParam = imageReader getDefaultReadParam

  val blockSize = 50000000
  val tileRange = findRange(if (imageWidth < imageHeight) imageWidth else imageHeight)
  aquire


  /**
   *
   */
  def findRange(start: Int): Int = if (start * start * 3 <= blockSize) start else findRange(start - 1)

  /**
   *
   */
  def aquire {

    doAppend[Text, BytesWritable]("hdfs://localhost:8022/user/training/", "ImageSequenceFile") {
      (sf, k, v) =>

        for { i <- 0 until imageWidth / tileRange
              j <- 0 until imageHeight / tileRange } {
          val tileWidth = if (i == imageWidth / tileRange) tileRange + imageWidth % tileRange else tileRange
          val tileHeight = if (j == imageHeight / tileRange) tileRange + imageHeight % tileRange else tileRange

          readParam setSourceRegion (new Rectangle(i * tileRange, j * tileRange, tileWidth, tileHeight))

          val tile = imageReader read (imageIndex, readParam)

          /**
           * writing file
           */
          val tileArray = new Array[Int](tile.getWidth * tile.getHeight)
          val imageArray = tile.getRGB(0, 0, tile.getWidth, tile.getHeight, tileArray, 0, 0)
          val key = "img_tile_" + i + "_" + j + ": " +  tile.getWidth + " " + tile.getHeight

          import Conversions._
          k.set(key)
          v.set(imageArray, 0, imageArray.length * 4)
          sf += (k, v)
        }
    }
  }
}