package com.oppo.tagbase.query;

import com.google.inject.Inject;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author huangfeng
 * @date 2020/2/10 13:26
 */
public class IdGenerator {

    private static DateTimeFormatter ID_DATE_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;


    private AtomicInteger count = new AtomicInteger(0);
    private final int serverId;

    private volatile  LocalDate lastEvalDate;


    @Inject
    public IdGenerator(int serverId){
        this.serverId = serverId;
        lastEvalDate = LocalDate.now();
    }

    /**
     * queryid format:
     *  date_serverId_id(id is increment count, which roll from 1 every day )
     */
    public String getNextId(){

        LocalDate now = LocalDate.now();

        if(lastEvalDate.isBefore(now)){
            lastEvalDate = now;
            count.set(0);
        }

        return String.format("%s_%02d_%06d",now.format(ID_DATE_FORMATTER),serverId,count.incrementAndGet());
    }


    public static void main(String[] args) {
       IdGenerator generator =  new IdGenerator(1);
       System.out.println(generator.getNextId());
       System.out.println(generator.getNextId());
       System.out.println(generator.getNextId());
    }
}
