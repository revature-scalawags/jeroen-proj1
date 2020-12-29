package wikianswers

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class WordMapper extends Mapper[LongWritable, Text, Text, IntWritable]{

    override def map(key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, IntWritable]#Context
    ): Unit ={
        val line = value.toString

    line
      .split("\\W+")
      .filter(_.length > 0)
      .map(_.toUpperCase)
      .foreach((word: String) => {
        context.write(new Text(word), new IntWritable(1))
      })
    }

}