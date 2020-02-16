package com.oppo.tagbase.storage.hbase.example;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.*;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.example.testobj.*;
import com.oppo.tagbase.storage.core.lifecycle.StorageLifecycleModule;
import com.oppo.tagbase.storage.hbase.HbaseStorageModule;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseStorageExmple {

    public static void main(String[] args) {

        Injector ij = ExampleGuiceInjectors.makeInjector(
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new ValidatorModule(),
                new HbaseStorageModule(),
                new LifecycleModule(),
                new StorageLifecycleModule(),
                new MetadataModule()
        );

        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);
        lifecycle.start();
        StorageConnector connector = ij.getInstance(StorageConnector.class);
        Metadata metadata = ij.getInstance(Metadata.class);

        try {

//            addEventData(connector);
//            addTagData(connector);
//            addFlowData(connector);

            metadata.setType(Metadata.MetaDataType.EVENT);
            SingleQuery query = queryEventData(connector);

//            metadata.setType(Metadata.MetaDataType.TAG);
//            SingleQuery query = queryTagData(connector);

            OperatorBuffer buffer = connector.createQuery(query);
            while (buffer.hasNext()){
                System.out.println("main:"+buffer.next());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            lifecycle.stop();
        }

    }


    public static SingleQuery queryEventData(StorageConnector connector){
        SingleQuery query = new SingleQuery();
        List<Filter> filters = new ArrayList<>();

        InFilter f = new InFilter("app",new ArrayList<String>(){{add("tenxun");add("wechat");}});filters.add(f);
        GreaterFilter f2 = new GreaterFilter("daynum","20200208");filters.add(f2);
        InFilter f3 = new InFilter("event",new ArrayList<String>(){{add("install");add("uninstall");}});filters.add(f3);
        //InFilter f4 = new InFilter("version",new ArrayList<String>(){{add("5.4");}});filters.add(f4);

        List<String> dims = new ArrayList<String>(){{add("app");add("event");add("version");add("daynum");}};

        query.setFilters(filters);
        query.setDimensions(dims);

        return query;
    }


    public static SingleQuery queryTagData(StorageConnector connector){
        SingleQuery query = new SingleQuery();
        List<Filter> filters = new ArrayList<>();

        InFilter f = new InFilter("city",new ArrayList<String>(){{add("shenzhen");}});
        filters.add(f);
        List<String> dims = new ArrayList<String>(){{}};

        query.setFilters(filters);
        query.setDimensions(dims);

        return query;
    }


    public static void addEventData(StorageConnector connector) throws IOException {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(1);bitmap.add(6);
        MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
        bitmap2.add(2);
        MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
        bitmap3.add(3);bitmap3.add(5);

        String tName = "event_20200209";
        String tName2 = "event_20200210";
        String tName3 = "event_20200211";

        connector.deleteTable(tName);
        connector.deleteTable(tName2);
        connector.deleteTable(tName3);
        connector.createTable(tName);
        connector.createTable(tName2);
        connector.createTable(tName3);

        connector.createRecord(tName, "1_wechat_install_5.2", bitmap);
        connector.createRecord(tName, "1_qq_install_5.1", bitmap2);
        connector.createRecord(tName, "1_wechat_uninstall_5.3", bitmap3);
        connector.createRecord(tName2, "1_wechat_install_5.4", bitmap);
        connector.createRecord(tName2, "1_qq_install_5.5", bitmap2);
        connector.createRecord(tName2, "1_wechat_uninstall_5.6", bitmap3);
        connector.createRecord(tName3, "1_oppo_install_5.4", bitmap);
        connector.createRecord(tName3, "1_oppo_login_5.4", bitmap2);
        connector.createRecord(tName3, "1_oppo_uninstall_5.4", bitmap3);
    }


    public static void addTagData(StorageConnector connector) throws IOException {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(3);bitmap.add(2);
        MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
        bitmap2.add(5);
        MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
        bitmap3.add(7);


        String tName = "city_20200209";
        String tName2 = "city_20200210";

        connector.deleteTable(tName);
        connector.deleteTable(tName2);
        connector.createTable(tName);
        connector.createTable(tName2);


        connector.createRecord(tName, "1_beijing", bitmap);
        connector.createRecord(tName, "1_shanghai", bitmap2);
        connector.createRecord(tName, "1_shenzhen", bitmap3);
        connector.createRecord(tName2, "1_guangzhou", bitmap);
        connector.createRecord(tName2, "1_shenzhen", bitmap2);
        connector.createRecord(tName2, "1_tianjin", bitmap3);
    }

    public static void addFlowData(StorageConnector connector) throws IOException {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(1);bitmap.add(6);
        MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
        bitmap2.add(8);
        MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
        bitmap3.add(2);

        String tName = "flow_20200209";
        String tName2 = "flow_20200210";
        String tName3 = "flow_20200211";

        connector.deleteTable(tName);
        connector.deleteTable(tName2);
        connector.deleteTable(tName3);
        connector.createTable(tName);
        connector.createTable(tName2);
        connector.createTable(tName3);


        connector.createRecord(tName, "1_baidu_vivo", bitmap);
        connector.createRecord(tName, "1_vivo_oppo", bitmap2);
        connector.createRecord(tName, "1_tenxun_qq", bitmap3);
        connector.createRecord(tName, "1_baidu_vivo", bitmap);
        connector.createRecord(tName, "1_baidu_qq", bitmap2);
        connector.createRecord(tName, "1_baidu_taobao", bitmap3);
        connector.createRecord(tName3, "1_tenxun_qq", bitmap);
        connector.createRecord(tName3, "1_tenxun_oppo", bitmap2);
        connector.createRecord(tName3, "1_vivo_oppo", bitmap3);
    }

}
