package com.oppo.tagbase.query.row;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class Dimensions {

    byte[][] internalDimensionValues;

    public Dimensions(byte[][] value) {
        this.internalDimensionValues = value;
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


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        for (byte[] value : internalDimensionValues) {
            builder.append(new String(value));
        }
        return builder.toString();
    }

    public String getSignature() {
        return null;
    }


}
