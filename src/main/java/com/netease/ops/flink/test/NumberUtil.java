package com.netease.ops.flink.test;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;


/**
 * 一些跟数字相关的util方法
 * @author hzyefeng
 */
public class NumberUtil {
    
    public static long BYTE1_LONG_MAX=getMaxLong(1);
    public static long BYTE1_LONG_MIN=getMinLong(1);
    
    public static long BYTE2_LONG_MAX=getMaxLong(2);
    public static long BYTE2_LONG_MIN=getMinLong(2);
    
    public static long BYTE3_LONG_MAX=getMaxLong(3);
    public static long BYTE3_LONG_MIN=getMinLong(3);
    
    public static long BYTE4_LONG_MAX=getMaxLong(4);
    public static long BYTE4_LONG_MIN=getMinLong(4);
    
    public static long BYTE5_LONG_MAX=getMaxLong(5);
    public static long BYTE5_LONG_MIN=getMinLong(5);
    
    public static long BYTE6_LONG_MAX=getMaxLong(6);
    public static long BYTE6_LONG_MIN=getMinLong(6);
    
    public static long BYTE7_LONG_MAX=getMaxLong(7);
    public static long BYTE7_LONG_MIN=getMinLong(7);
    
    public static long BYTE8_LONG_MAX=getMaxLong(8);
    public static long BYTE8_LONG_MIN=getMinLong(8);

    /*
     * 计算以补码表示的多个字节的整数的最大值
     */
    public static long getMaxLong(int numberOfBytes){
        long a=1;
        for(int i =0;i<numberOfBytes*8-1;i++){
            a=a*2;
        }
        return a-1;
    }
    
    /*
     * 计算以补码表示的多个字节的整数的最小值
     */
    public static long getMinLong(int numberOfBytes){
        long a=-1;
        for(int i =0;i<numberOfBytes*8-1;i++){
            a=a*2;
        }
        return a;
    }
    
    

    /**
     * 将long值转换成二进制的表示，根据long值的大小转换成不同字节大小的二进制
     * @param value，需要转换的整数
     * @return 转换后的二进制数组
     */
    public static byte[] encodeLong(long value) throws IllegalArgumentException {
        if(value<=BYTE1_LONG_MAX && value>=BYTE1_LONG_MIN){
            byte bv=(byte)value;
            return new byte[]{bv};
        }else if(value<=BYTE2_LONG_MAX && value>=BYTE2_LONG_MIN){
            short sv= (short)value;
            return Bytes.toBytes(sv);
        }else if(value<=BYTE4_LONG_MAX && value>=BYTE4_LONG_MIN){
            int iv=(int)value;
            return Bytes.toBytes(iv);
        }else if(value<=BYTE8_LONG_MAX && value>=BYTE8_LONG_MIN){
            return Bytes.toBytes(value);
        }
        throw new IllegalArgumentException("impossible");

    }
    
    
    public static int encodedLongSize(long value) throws IllegalArgumentException {
        if(value<=BYTE1_LONG_MAX && value>=BYTE1_LONG_MIN){
           return 1;
        }else if(value<=BYTE2_LONG_MAX && value>=BYTE2_LONG_MIN){
            
            return 2;
        }else if(value<=BYTE4_LONG_MAX && value>=BYTE4_LONG_MIN){
           
            return 4;
        }else if(value<=BYTE8_LONG_MAX && value>=BYTE8_LONG_MIN){
            return 8;
        }
        throw new IllegalArgumentException("impossible");
    }
    
    /**
     * 将二进制转换成整数值,根据二进制数组的长度自动换成相应的类型
     * @param bytes 需要转换的二进制数组
     * @return 转换后的整数
     */
    public static long decodeLong(byte[]bytes) throws IllegalArgumentException {
        if(bytes.length==1){
            byte b=bytes[0];
            return (long)b;
        }else if(bytes.length==2){
            short sv= Bytes.toShort(bytes);
            return (long)sv;
        }else if(bytes.length==4){
            int iv= Bytes.toInt(bytes);
            return (long)iv;
        }else if(bytes.length==8){
            return Bytes.toLong(bytes);
        }else{
            throw new IllegalArgumentException("invalid length for long value: "+bytes.length);
        }
    }
    
    /**
     * 将二进制转换成整数值,根据二进制数组的长度自动换成相应的类型
     * @param bytes 需要转换的二进制数组
     * @return 转换后的整数
     */
    public static long decodeLong(byte[]bytes,int offset,int length) throws IllegalArgumentException {
        if(length==1){
            byte b=bytes[offset];
            return (long)b;
        }else if(length==2){
            short sv= Bytes.toShort(bytes,offset);
            return (long)sv;
        }else if(length==4){
            int iv= Bytes.toInt(bytes,offset);
            return (long)iv;
        }else if(length==8){
            return Bytes.toLong(bytes,offset);
        }else{
            throw new IllegalArgumentException("invalid length for long value: "+bytes.length);
        }
    }
    
    /**
     * 
     * 将long值的低五个字节put到bytes数据,对于以秒为单位的时间，用5个字节标示就足够了，需要转换的值一定要大于0
     */
    public static int put5bytesLong(byte[] bytes, int offset, long val) {
        if(val<0){
            throw new IllegalArgumentException("must greater than 0");
        }

        if (bytes.length - offset < 5) {
          throw new IllegalArgumentException("Not enough room to put a 5 byte long at"
              + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        for(int i = offset + 4; i > offset; i--) {
          bytes[i] = (byte) val;
          val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + 5;
    }
    
    /*
     * 将bytes 的offset开始的5个字解转换成long值
     */
    public static long to5byteLong(byte[] bytes,int offset){
        if (offset + 5 > bytes.length) {
            throw new IllegalArgumentException("not enough bytes for 5 bytes long");
        }
        
        long l = 0;
        for(int i = offset; i < offset + 5; i++) {
          l <<= 8;
          l ^= bytes[i] & 0xFF;
        }
        return l;
    }
    /**
     *
     * 将long值的低六个字节put到bytes数据,对于以毫秒为单位的时间
     */
    public static int put6bytesLong(byte[] bytes, int offset, long val) {
        if(val<0){
            throw new IllegalArgumentException("must greater than 0");
        }
        //不能超过6字节整数的最大值
        if(val>BYTE6_LONG_MAX){
            throw new IllegalArgumentException("value exceeds the max value for a 6 byte long,max:"+BYTE5_LONG_MAX+",this:"+val);
        }
        if (bytes.length - offset < 6) {
            throw new IllegalArgumentException("Not enough room to put a 5 byte long at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        for(int i = offset + 5; i > offset; i--) {
            bytes[i] = (byte) val;
            val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + 6;
    }

    /*
     * 将bytes 的offset开始的5个字解转换成long值
     */
    public static long to6bytseLong(byte[] bytes,int offset){
        if (offset + 6 > bytes.length) {
            throw new IllegalArgumentException("not enough bytes for 6 bytes long");
        }

        long l = 0;
        for(int i = offset; i < offset + 6; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    
    
    /*
     * 将整数类型的，Long，cast成Long对象
     * 将浮点类型的，Float，Double cast成Double对象
     */
    public static Number castToLongOrDouble(Object co) throws IllegalArgumentException {

       if(co instanceof Long){
            return (Long)co;
        }else if(co instanceof Float){
            Float fvalue=(Float)co;
            return fvalue.doubleValue();
        }else if(co instanceof Double){
            return (Double)co;
        }else{
            throw new IllegalArgumentException("not supported data type");
        }
    }
    
   
    /*
     * 将对象转换成Long类型那个的
     */
    public static Long castToLong(Object co) throws IllegalArgumentException {
        if(co instanceof Long){
            return (Long)co;
        }else if(co instanceof Integer){
            Integer ivalue=(Integer)co;
            return ivalue.longValue();
        }else{
            throw new IllegalArgumentException("not supported data type:"+co.getClass().getName());
        }
    }
    
    /*
     * 将对象转换成double类型
     */
    public static Double castToDouble(Object co) throws IllegalArgumentException {
        if(co instanceof Float){
            Float fvalue=(Float)co;
            return fvalue.doubleValue();
        }else if(co instanceof Double){
            return (Double)co;
        }else{
            throw new IllegalArgumentException("not supported data type:"+co.getClass().getName());
        }
    }
 
    /*
     * 两个对象相减，前提是减数大于被减数，否者返回null
     */
    public static Object subtract(Object o1,Object o2) throws IllegalArgumentException {
        if(o1==null || o2 ==null){
            throw new NullPointerException("o1 or o2 is null");
        }
        if(o1 instanceof Long){
            Long l1=(Long)o1;
            Long l2=(Long)o2;
            if(l1.longValue()>=l2.longValue()){
                return Long.valueOf(l1.longValue()-l2.longValue());
            }else{
                return null;
            }
        }else if(o1 instanceof Float){
            Float f1=(Float)o1;
            Float f2=(Float)o2;
            if(f1.floatValue()>=f2.floatValue()){
                return Float.valueOf(f1.floatValue()-f2.floatValue());
            }else{
                return null;
            }
        }else if(o2 instanceof Double){
            Double d1=(Double)o1;
            Double d2=(Double)o2;
            if(d1.doubleValue()>=d2.doubleValue()){
                return Double.valueOf(d1.doubleValue()-d2.doubleValue());
            }else{
                return null;
            }
        }else{
            throw new IllegalArgumentException("should not happen!");
        }
        
    }
    
    /*
     * 两个对象相加
     */
    public static Object add(Object o1,Object o2) throws IllegalArgumentException {
        if(o1==null || o2 ==null){
            throw new NullPointerException("o1 or o2 is null");
        }
        if(o1 instanceof Long){
            Long l1=(Long)o1;
            Long l2=(Long)o2;
            return Long.valueOf(l1.longValue()+l2.longValue());
        }else if(o1 instanceof Float){
            Float f1=(Float)o1;
            Float f2=(Float)o2;
            return Float.valueOf(f1.floatValue()+f2.floatValue());
        }else if(o2 instanceof Double){
            Double d1=(Double)o1;
            Double d2=(Double)o2;
            return Double.valueOf(d1.doubleValue()+d2.doubleValue());
        }else{
            throw new IllegalArgumentException("should not happen!");
        }
        
    }
    
    
    
    public static boolean equal(Number a, Number b){
    	if(a==null){
    		if(b==null){
    			return true;
    		}else{
    			return false;
    		}
    	}else{
    		if(b==null){
    			return false;
    		}else{
    			return a.equals(b);
    		}
    	}
    }



    
    
    
    
    
    
    public static void main(String args[]) throws IOException {

        System.out.println("1 byte["+BYTE1_LONG_MIN+","+BYTE1_LONG_MAX+"]");
        System.out.println("2 byte["+BYTE2_LONG_MIN+","+BYTE2_LONG_MAX+"]");
        System.out.println("3 byte["+BYTE3_LONG_MIN+","+BYTE3_LONG_MAX+"]");
        System.out.println("4 byte["+BYTE4_LONG_MIN+","+BYTE4_LONG_MAX+"]");
        System.out.println("5 byte["+BYTE5_LONG_MIN+","+BYTE5_LONG_MAX+"]");
        System.out.println("6 byte["+BYTE6_LONG_MIN+","+BYTE6_LONG_MAX+"]");
        System.out.println("7 byte["+BYTE7_LONG_MIN+","+BYTE7_LONG_MAX+"]");
        System.out.println("8 byte["+BYTE8_LONG_MIN+","+BYTE8_LONG_MAX+"]");
        
        long v=9223372036854775807l;
        System.out.println("begin:"+v);
        byte[] bb=encodeLong(v);
        long r=decodeLong(bb);
        System.out.println("result:"+r);
        
        ByteArrayOutputStream bstream=new ByteArrayOutputStream(8);
        
        DataOutput  output=new DataOutputStream(bstream);
        
        WritableUtils.writeVLong(output, v);
        byte bbbb[]=bstream.toByteArray();
        System.out.println("length:"+bbbb.length);
        ByteArrayInputStream instrem=new ByteArrayInputStream(bbbb);
        DataInputStream instream=new DataInputStream(instrem);
        long value= WritableUtils.readVLong(instream);
        System.out.println("VLong Stream:"+value);
        
    }
    
    
}
