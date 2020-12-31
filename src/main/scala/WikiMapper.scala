package wikianswers

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class WikiMapper extends Mapper[LongWritable, Text, Text, IntWritable]{

    override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, IntWritable]#Context
  ): Unit = {
    val line = value.toString

    val lineArray = line.split("\\s").filter(_.length > 0)
    var destination = ""
    var checker = false 
      if (lineArray(2) == "link") checker = true
      if (checker == true)  {
        destination = lineArray(0)
        context.write(new Text(destination), new IntWritable(1))
        checker = false
      }
  }
}