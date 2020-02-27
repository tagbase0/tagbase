package com.oppo.tagbase.storage.hbase.example;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.*;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.example.testobj.*;
import com.oppo.tagbase.storage.core.exception.StorageException;
import com.oppo.tagbase.storage.core.lifecycle.StorageLifecycleModule;
import com.oppo.tagbase.storage.core.obj.*;
import com.oppo.tagbase.storage.hbase.HbaseStorageModule;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class HbaseStorageExample {

    public static void main(String[] args) {

        Injector ij = ExampleGuiceInjectors.makeInjector(
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new ValidatorModule(),
                new HbaseStorageModule(),
                new LifecycleModule(),
                new StorageLifecycleModule(),
                new TestMetadataModule()
        );

        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);
        lifecycle.start();
        StorageConnector connector = ij.getInstance(StorageConnector.class);
        TestMetadata metadata = ij.getInstance(TestMetadata.class);

        try {

//            addEventData(connector);
//            addTagData(connector);
//            addFlowData(connector);

            metadata.setType(TestMetadata.MetaDataType.EVENT);
            QueryHandler query = queryEventData(connector);

//            metadata.setType(TestMetadata.MetaDataType.TAG);
//            QueryHandler query = queryTagData(connector);

            OperatorBuffer<RawRow> buffer = connector.createQuery(query);
            RawRow row = null;
            while ((row = buffer.next()) != null){
                System.out.println("OperatorBuffer: " + row);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            lifecycle.stop();
        }

    }


    public static QueryHandler queryEventData(StorageConnector connector){

        List<ColumnDomain<String>> dimQueryList = new ArrayList<>();
        RangeSet<String> appColumnRange = TreeRangeSet.create();
        appColumnRange.add(Range.singleton("tenxun"));appColumnRange.add(Range.singleton("wechat"));
        dimQueryList.add(new ColumnDomain<String>(appColumnRange, "app" ));

        RangeSet<String> eventColumnRange = TreeRangeSet.create();
        eventColumnRange.add(Range.singleton("install"));
        dimQueryList.add(new ColumnDomain<String>(eventColumnRange, "event"));

        RangeSet<String> versionColumnRange = TreeRangeSet.create();
        versionColumnRange.add(Range.singleton("5.4"));versionColumnRange.add(Range.singleton("5.2"));
        dimQueryList.add(new ColumnDomain<String>(versionColumnRange, "version"));

        RangeSet<LocalDateTime> sliceRange = TreeRangeSet.create();
        sliceRange.add(Range.lessThan(LocalDateTime.now()));
        ColumnDomain<LocalDateTime> sliceQuery = new ColumnDomain<LocalDateTime>(sliceRange,"daynum");

        List<String> dims = new ArrayList<String>(){{add("app");add("event");add("version");add("daynum");}};
      //  List<String> dims = null;

        QueryHandler query = new QueryHandler("default","event_20200210",dims,dimQueryList,sliceQuery,"2139872645714");

        return query;
    }


    public static QueryHandler queryTagData(StorageConnector connector){

        List<ColumnDomain<String>> dimQueryList = new ArrayList<>();
        RangeSet<String> appColumnRange = TreeRangeSet.create();
        appColumnRange.add(Range.singleton("beijing"));appColumnRange.add(Range.singleton("shenzhen"));
        dimQueryList.add(new ColumnDomain<String>(appColumnRange, "city"));

        List<String> dims = new ArrayList<String>(){{add("city");}};
        //  List<String> dims = null;

        QueryHandler query = new QueryHandler("default","city_20200210",dims,dimQueryList,null,"2139872645714");

        return query;
    }

    public static void addEventData(StorageConnector connector) throws IOException, StorageException {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(1);bitmap.add(6);
        MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
        bitmap2.add(2);
        MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
        bitmap3.add(3);bitmap3.add(5);

        String tName = "event_20200209";
        String tName2 = "event_20200210";
        String tName3 = "event_20200211";

        String nameSpace = "tagbase";

        connector.deleteTable(nameSpace,tName);
        connector.deleteTable(nameSpace,tName2);
        connector.deleteTable(nameSpace,tName3);
        connector.createTable(nameSpace,tName);
        connector.createTable(nameSpace,tName2);
        connector.createTable(nameSpace,tName3);

        connector.createRecord(nameSpace,tName, "1_wechat_install_5.2", bitmap);
        connector.createRecord(nameSpace,tName, "1_qq_install_5.1", bitmap2);
        connector.createRecord(nameSpace,tName, "1_wechat_uninstall_5.3", bitmap3);
        connector.createRecord(nameSpace,tName2, "1_wechat_install_5.4", bitmap);
        connector.createRecord(nameSpace,tName2, "1_qq_install_5.5", bitmap2);
        connector.createRecord(nameSpace,tName2, "1_wechat_uninstall_5.6", bitmap3);
        connector.createRecord(nameSpace,tName3, "1_oppo_install_5.4", bitmap);
        connector.createRecord(nameSpace,tName3, "1_oppo_login_5.4", bitmap2);
        connector.createRecord(nameSpace,tName3, "1_oppo_uninstall_5.4", bitmap3);
    }


    public static void addTagData(StorageConnector connector) throws IOException, StorageException {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(3);bitmap.add(2);
        MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
        bitmap2.add(5);
        MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
        bitmap3.add(7);


        String tName = "city_20200209";
        String tName2 = "city_20200210";

        String nameSpace = "tagbase";

        connector.deleteTable(nameSpace,tName);
        connector.deleteTable(nameSpace,tName2);
        connector.createTable(nameSpace,tName);
        connector.createTable(nameSpace,tName2);


        connector.createRecord(nameSpace,tName, "1_beijing", bitmap);
        connector.createRecord(nameSpace,tName, "1_shanghai", bitmap2);
        connector.createRecord(nameSpace,tName, "1_shenzhen", bitmap3);
        connector.createRecord(nameSpace,tName2, "1_guangzhou", bitmap);
        connector.createRecord(nameSpace,tName2, "1_shenzhen", bitmap2);
        connector.createRecord(nameSpace,tName2, "1_tianjin", bitmap3);
    }

    public static void addFlowData(StorageConnector connector) throws IOException, StorageException {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(1);bitmap.add(6);
        MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
        bitmap2.add(8);
        MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
        bitmap3.add(2);

        String tName = "flow_20200209";
        String tName2 = "flow_20200210";
        String tName3 = "flow_20200211";

        String nameSpace = "tagbase";

        connector.deleteTable(nameSpace,tName);
        connector.deleteTable(nameSpace,tName2);
        connector.deleteTable(nameSpace,tName3);
        connector.createTable(nameSpace,tName);
        connector.createTable(nameSpace,tName2);
        connector.createTable(nameSpace,tName3);


        connector.createRecord(nameSpace,tName, "1_baidu_vivo", bitmap);
        connector.createRecord(nameSpace,tName, "1_vivo_oppo", bitmap2);
        connector.createRecord(nameSpace,tName, "1_tenxun_qq", bitmap3);
        connector.createRecord(nameSpace,tName, "1_baidu_vivo", bitmap);
        connector.createRecord(nameSpace,tName, "1_baidu_qq", bitmap2);
        connector.createRecord(nameSpace,tName, "1_baidu_taobao", bitmap3);
        connector.createRecord(nameSpace,tName3, "1_tenxun_qq", bitmap);
        connector.createRecord(nameSpace,tName3, "1_tenxun_oppo", bitmap2);
        connector.createRecord(nameSpace,tName3, "1_vivo_oppo", bitmap3);
    }

}
