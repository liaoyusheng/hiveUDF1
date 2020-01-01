package com.lys;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* 简单的数据清理
* 1、剔除长度不够的日志行
* 2、剔除 url 行
* 3、provinceId 为非数字行
*
* */
public class MyMap extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        if (splits.length < 32) {
            context.getCounter("etl", "数据长度不够！").increment(1L);
            return;
        }
        String url = splits[1];
        if (url == null || url.length() <= 0) {
            context.getCounter("elturl", "url is blank").increment(1L);
        }

        String provinceId = splits[20];
        int provinceIdInt = Integer.MAX_VALUE;
        try {
            provinceIdInt = Integer.parseInt(provinceId);
        } catch (Exception e) {
            context.getCounter("elt provinceId", "provinceId is unknown").increment(1L);
            return;
        }

        if (StringUtils.isBlank(provinceId) || provinceIdInt == Integer.MAX_VALUE) {
            context.getCounter("elt provinceId", "provinceId is unknown").increment(1L);
            return;
        }

        context.write(key, value);

    }
}
