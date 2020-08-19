package com.atguigu.utils;
import java.util.Random;
/**
 * Author: doubleZ
 * Datetime:2020/8/14   19:22
 * Description:
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}
