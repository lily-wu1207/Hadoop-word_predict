package com.mapreduce.wordcount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import PreJob.WordCount;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WordcountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // PreJob
        Configuration conf = new Configuration();

        String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("必须输入读取文件路径和输出路径");
            System.exit(2);
        }
        Path inputPath = new Path(otherArgs[0]);
        Path tmpPath = new Path("tmp");
        Path outputPath = new Path(otherArgs[1]);
        // 删除遗留的输出文件
        FileSystem fs =FileSystem.get(conf);
        if(fs.exists(tmpPath)){
            fs.delete(tmpPath,true);
        }
        if(fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        long CorpusSize = WordCount.getCorpusSize(conf, inputPath, tmpPath);  // 先通过一个job来获取语料库大小（用于smoothing）
        System.out.println("CorpusSize = " + CorpusSize);

        Date date1 = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String sim = dateFormat.format(date1);
        System.out.println(sim);

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
//        job.setInputFormatClass(NLineInputFormat.class);
//        NLineInputFormat.setNumLinesPerSplit(job,5);
//        NLineInputFormat.setInputPaths(job,new Path("F:大数据笔记Day02-Hadoop数据文件flow.txt")); // 其代码均和WC一致


        // 2 设置jar加载路径
        job.setJarByClass(WordcountDriver.class);




        job.setOutputKeyClass(Text.class);                 // 设置reduce函数的key值
        job.setOutputValueClass(MyMapWritable.class);        // 设置reduce函数的value值

        // 3 设置map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setCombinerClass(WordcountCombiner.class);
        job.setReducerClass(WordcountReducer.class);

        // 4 设置map输出
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(MyMapWritable.class);

        // 5 设置最终输出kv类型
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.addInputPath(job, inputPath);      // 设置读取文件的路径，从HDFS中读取
        FileOutputFormat.setOutputPath(job, outputPath);   // 设置mapreduce程序的输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        Path tmpPath = new Path("tmp");
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));


//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        boolean result = job.waitForCompletion(true);
        if(result) {
            Date date2 = new Date();
            SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            String sim2 = dateFormat.format(date2);
            System.out.println(sim2);
            //两个时间之毫秒差
            try {
                Date a = dateFormat.parse(sim);
                Date b = dateFormat.parse(sim2);
                long mm = b.getTime() - a.getTime();
                System.out.println(mm);
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        System.exit(result ? 0 : 1);

        // 7 提交
//        boolean result = job.waitForCompletion(true);
//
//        System.exit(result ? 0 : 1);
    }
}


