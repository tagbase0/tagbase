package com.oppo.tagbase.query.operator;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class Dimensions {

    byte[][] interalDimensionValues;

    public  Dimensions(byte[][] value){
        this.interalDimensionValues = value;
    }

    public String getString(int index) {
        return new String(interalDimensionValues[index]);
    }

    public int getInt(int index) {
        return 1;
    }

    public int length() {
        return interalDimensionValues.length;
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        for (byte[] value : interalDimensionValues) {
            builder.append(new String(value));
        }
        return builder.toString();
    }
}
