package com.oppo.tagbase.dict;

/**
 * Created by wujianchao on 2020/2/11.
 */
public interface Dictionary {

    /**
     * @return total element num
     */
    long elementNum();


    /**
     * search element by id
     */
    byte[] element(long id);


    /**
     *search id by element
     */
    long id(byte[] element);

}
