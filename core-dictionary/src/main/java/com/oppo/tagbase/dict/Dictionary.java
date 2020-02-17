package com.oppo.tagbase.dict;

/**
 * Created by wujianchao on 2020/2/11.
 */
public interface Dictionary {

    /**
     * 获取下一个值的编号。对于正向字典来说下一个值得编号以加入字典的顺序为准。
     * 因为字典值得编号从0开始，所以下一个值的编号取值为字典的值的个数。
     *
     * 当向字典添加数据的时候需要用到该方法。
     *
     * @return next element id
     */
    int nextId();


    /**
     * 字典的正向查找，查找编号对应的值
     *
     * @param id 编号
     * @return id对应的值
     */
    byte[] element(int id);


    /**
     *字典的反向查找，查找值对应的编号
     *
     * @param element 值
     * @return element对应的编号
     */
    int id(byte[] element);

}
