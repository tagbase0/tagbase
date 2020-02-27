package com.oppo.tagbase.storage.core.obj;


import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class Dimensions {

    private static  final byte[][] EMPTY_BYTES = new byte[0][];

    byte[][] internalDimensionValues;

    public Dimensions(byte[][] value) {
       this.internalDimensionValues = value!=null?value:EMPTY_BYTES;
    }


    public static Dimensions join(Dimensions dim1, Dimensions dim2) {

        byte[][] values = new byte[dim1.length() + dim2.length()][];
        for (int n = 0; n < dim1.length(); n++) {
            values[n] = dim1.getBytes(n);
        }
        for (int n = dim1.length(); n < dim1.length() + dim2.length(); n++) {
            values[n] = dim2.getBytes(n - dim1.length());
        }

        return new Dimensions(values);

    }

    private byte[] getBytes(int n) {
        return internalDimensionValues[n];
    }

    public String getString(int index) {
        return new String(internalDimensionValues[index]);
    }


    public int length() {
        return internalDimensionValues.length;
    }

    public static void main(String[] args) {
        byte[][] a = new byte[0][];

        System.out.println(new Dimensions(a).toString());
    }

    @Override
    public String toString() {
      return Arrays.stream(internalDimensionValues).map(str -> new String(str)).collect(Collectors.toList()).toString();
    }

    public String getSignature() {
        return null;
    }


}
