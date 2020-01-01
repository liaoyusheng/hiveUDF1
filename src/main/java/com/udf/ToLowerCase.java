package com.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/*
* hive 用户自定义函数
* 
* */
public class ToLowerCase extends UDF {

    public Text evaluate(Text str) {
        if (null == str) {
            return null;
        } else if (str != null && str.getLength() <= 0) {
            return null;
        }

        return new Text(str.toString().toLowerCase());
    }

    public static void main(String[] arg) {
        System.out.println(new ToLowerCase().evaluate(new Text(arg[0])));
    }
}
