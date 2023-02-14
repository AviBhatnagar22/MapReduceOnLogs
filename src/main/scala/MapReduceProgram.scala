import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.time.LocalTime
import java.util
import scala.util.matching.Regex
import scala.jdk.CollectionConverters.*
import scala.runtime.Nothing$


object MapReduceProgram:
  val startTime: LocalTime = LocalTime.parse("11:42:50.878")
  val endTime: LocalTime = LocalTime.parse("11:43:28.550")
  val pattern = new Regex("^\\S{1,100}$")

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()


    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val arr: Array[String] = line.split("\\s+", 6)
      val lt: LocalTime = LocalTime.parse(arr.apply(0))
      val afterStart: Int = lt.compareTo(startTime)
      val beforeEnd: Int = lt.compareTo(endTime)
      val mt = pattern.findFirstIn(arr.apply(5))

      if (afterStart >= 0 && beforeEnd < 0 && mt.isDefined) {
        word.set(arr.apply(2))
        output.collect(word, one)
      }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  class Task3Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line = value.toString
      val arr = line.split(" ", 7)
      word.set(arr.apply(2))
      output.collect(word, one)

  class Task3Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))



  @main def runMapReduce(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("Task1")
    //    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.set("mapreduce.output.textoutputformat.separator", ",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)

////     Task 3
//    val conf2: JobConf = new JobConf(this.getClass)
//    conf2.setJobName("Task3")
//    //    conf.set("fs.defaultFS", "local")
//    conf2.set("mapreduce.job.maps", "1")
//    conf2.set("mapreduce.job.reduces", "1")
//    conf2.set("mapreduce.output.textoutputformat.separator", ",")
//    conf2.setOutputKeyClass(classOf[Text])
//    conf2.setOutputValueClass(classOf[IntWritable])
//    conf2.setMapperClass(classOf[Task3Map])
//    conf2.setCombinerClass(classOf[Task3Reduce])
//    conf2.setReducerClass(classOf[Task3Reduce])
//    conf2.setInputFormat(classOf[TextInputFormat])
//    conf2.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
//    FileInputFormat.setInputPaths(conf2, new Path(inputPath))
//    FileOutputFormat.setOutputPath(conf2, new Path(outputPath+"/task3"))
//    JobClient.runJob(conf2)