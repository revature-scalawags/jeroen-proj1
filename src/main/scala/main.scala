package wikianswers

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable




object Main extends App{
    
    if(args.length !=2){
        println("Usage: Main [input dir] [output dir] [mode]")
        println("for mode input 1 for Destination Counter, 2 for Source Counter")
        System.exit(-1)
    }

    val job = Job.getInstance()
    job.setJarByClass(Main.getClass())
    job.setJobName("wiki")
    job.setInputFormatClass(classOf[TextInputFormat])

    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    
    if(args(2).equals("1")){
        job.setMapperClass(classOf[WikiMapper])
    }else if(args(2).equals("2")){
        job.setMapperClass(classOf[WikiMapper2])
    }else{
        println("Invalid mode input")
        System.exit(2)
    }
    
    job.setReducerClass(classOf[WikiReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    val success = job.waitForCompletion(true)
    System.exit(if (success) 0 else 1)

}