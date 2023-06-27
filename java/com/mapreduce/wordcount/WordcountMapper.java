package com.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
//public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
//
//    Text k = new Text();
//    IntWritable v = new IntWritable(1);
//
//    @Override
//    protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
//
//        // 1 获取一行
//        String line = value.toString();
//
//        // 2 切割
//        String[] words = line.split(" ");
//
//        // 3 输出
//        for (String word : words) {
//
//            k.set(word);
//            context.write(k, v);
//        }
//    }
//}
public class WordcountMapper extends Mapper<LongWritable, Text, Text, MyMapWritable> {

    /**
     * map阶段主要逻辑
     * */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();
        System.out.println("line :" + line);
        solve(line.replace(" ", ""), context);  // 提取键值对



    }

    /**
     * 实现Stripes统计，即统计形如(ab，c)这样的tri-gram子串个数，记录到map中并发送
     * map的例子：ab -> {c:1, d:5, e:2,...}
     * */
    private void solve(String str, Context context) throws IOException, InterruptedException {
        HashMap<Text, MyMapWritable> map = new HashMap<>();
        HashMap<Text, MyMapWritable> map2 = new HashMap<>();
        //3-gram: ab -> {c:1, d:5, e:2,...}
        for(int i = 0; i < str.length() - 2; i ++){
            Text ab = new Text(str.substring(i, i + 2)), c = new Text(str.substring(i + 2, i + 3));
            if(str.charAt(i + 2) == ' ' || str.charAt(i + 2) == '\n' || str.charAt(i + 2) == '\r') {
                continue;  // 字符c是空格或换行的不考虑
            }
            if(!map.containsKey(ab)) {
                map.put(ab, new MyMapWritable());
            }
            update(map.get(ab), c);
//            System.out.println("map updated"+ab+c);
        }
        //4-gram: abc -> {c:1, d:5, e:2,...}
        for(int i = 0; i < str.length() - 3; i ++){
            Text abc = new Text(str.substring(i, i + 3)), d = new Text(str.substring(i + 3, i + 4));
            if(str.charAt(i + 3) == ' ' || str.charAt(i + 3) == '\n' || str.charAt(i + 3) == '\r') {
                continue;  // 字符c是空格或换行的不考虑
            }
            if(!map.containsKey(abc)) {
                map.put(abc, new MyMapWritable());
            }
            update(map.get(abc), d);
//            System.out.println("map updated"+ab+c);
        }
        for(Entry<Text, MyMapWritable> entry : map.entrySet()) {
//            System.out.println("write"+entry.getKey()+entry.getValue());
            context.write(entry.getKey(), entry.getValue());
//            System.out.println("map write");
        }
    }

    /**
     * 实现map的更新
     * */
    private void update(MyMapWritable val_map, Text key) {
        int old_val = (val_map.containsKey(key) ? ((IntWritable) val_map.get(key)).get(): 0);
        val_map.put(key, new IntWritable(old_val + 1));
    }
//    private void update2(MyMapWritable val_map, Text key) {
//        int old_val = (val_map.containsKey(key) ? ((IntWritable) val_map.get(key)).get(): 0);
//        val_map.put(key, new IntWritable(old_val + 1));
//    }
}


