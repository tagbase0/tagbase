package com.oppo.tagbase.storage.example;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.oppo.tagbase.guice.*;
import com.oppo.tagbase.storage.bean.BitmapBean;
import com.oppo.tagbase.storage.connector.StorageConnector;
import com.oppo.tagbase.storage.connector.StorageModule;
import com.oppo.tagbase.storage.lifecycle.ConnectorLifecycleModule;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.IOException;
import java.util.Iterator;

public class StorageExmple {

    public static void main(String[] args) {
        Injector ij = GuiceInjectors.makeInjector(
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new ValidatorModule(),
                new StorageModule(),
                new LifecycleModule(),
                new ConnectorLifecycleModule()
        );

        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);
        lifecycle.start();

        StorageConnector connector = ij.getInstance(StorageConnector.class);

        try {
            String tName = "event_20200210";
            Iterator<BitmapBean> iterator = connector.getRecords(tName);
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lifecycle.stop();
        }

    }

    public static void addData(StorageConnector connector) throws IOException {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        bitmap.add(2);
        bitmap.add(5);
        MutableRoaringBitmap bitmap2 = new MutableRoaringBitmap();
        bitmap2.add(3);
        MutableRoaringBitmap bitmap3 = new MutableRoaringBitmap();
        bitmap3.add(4);

        String tName = "event_20200210";
        String tName2 = "city_20200210";
        String tName3 = "flow_20200210";

        connector.addRecord(tName, "1_wechat_install_5.2", bitmap);
        connector.addRecord(tName, "1_qq_install_5.1", bitmap2);
        connector.addRecord(tName, "1_wechat_uninstall_5.3", bitmap3);
        connector.addRecord(tName2, "1_beijing", bitmap);
        connector.addRecord(tName2, "1_shanghai", bitmap2);
        connector.addRecord(tName2, "1_shenzhen", bitmap3);
        connector.addRecord(tName3, "1_wechat_qq", bitmap);
        connector.addRecord(tName3, "1_vivo_oppo", bitmap2);
        connector.addRecord(tName3, "1_qq_oppo", bitmap3);
    }

}
