package com.oppo.tagbase.meta;

import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.ExampleGuiceInjectors;
import com.oppo.tagbase.common.guice.PropsModule;
import com.oppo.tagbase.common.guice.ValidatorModule;
import com.oppo.tagbase.meta.connector.MetaStoreConnectorConfig;
import com.oppo.tagbase.meta.obj.Dict;
import com.oppo.tagbase.meta.obj.DictStatus;
import com.oppo.tagbase.meta.obj.DictType;
import org.junit.Before;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;




/**
 * Created by daikai on 2020/2/27.
 */
public class MetadataDictTest {

    MetadataDict metadataDict;
    @Before
    public void setup() {
        Injector injector = ExampleGuiceInjectors.makeInjector(
                new ValidatorModule(),
                new PropsModule(Lists.newArrayList("tagbase.properties")),
                new MetadataModule()
        );

        MetaStoreConnectorConfig c = injector.getInstance(MetaStoreConnectorConfig.class);

        metadataDict =injector.getInstance(MetadataDict.class);
        System.out.println(c.getUser());
    }

    
    public void addDict() {
        Dict dict = new Dict();
        dict.setVersion("2.0.0");
        dict.setType(DictType.FORWARD);
        dict.setElementCount(500000000);
        dict.setStatus(DictStatus.READY);
        dict.setLocation("/hive/osql/dict/forward1");
        dict.setCreateDate(LocalDateTime.parse("2020-02-04 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        metadataDict.addDict(dict);
    }

    
    public void getDict() {
        System.out.println(metadataDict.getDict());
    }

    
    public void getDictElementCount() {
        System.out.println(metadataDict.getDictElementCount());
    }
}