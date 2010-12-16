package hbaseimage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}

/**
 * @author Christopher Schmidt
 * @author Andreas Huelzenbecher
 *
 * The Hadoop job description
 */

class LargeImageProcessingJob

object LargeImageProcessingJob {

  /**
   * start job
   */
  def main(args: Array[String]):Unit = {

    val conf = new Configuration
    val job = new Job(conf)

    val fs = FileSystem.get(conf)
    fs.delete(new Path("output"), true)

    FileInputFormat.setInputPaths(job, "hdfs://localhost:8022/user/training/ImageSequenceFile")
    //      FileInputFormat.addInputPaths( job, "input/local1.tif" );
    FileOutputFormat.setOutputPath(job, new Path("output"))
    // Specify various job-specific parameters
    job.setJobName("ImageConverter");
    job.setJarByClass(classOf[LargeImageProcessingJob])
    job.setInputFormatClass(classOf[SequenceFileInputFormat[_,_]])
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_,_]])

    job.setMapperClass(classOf[GrayScaleMapper])
    job.setNumReduceTasks(0);

    job.waitForCompletion(true);
  }
}