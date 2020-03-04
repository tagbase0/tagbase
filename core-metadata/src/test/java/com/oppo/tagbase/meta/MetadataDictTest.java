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
import com.oppo.tagbase.meta.util.DateUtil;
import org.junit.Assert;
import org.junit.Before;


import java.time.format.DateTimeFormatter;


/**
 * Created by daikai on 2020/2/27.
 */
public class MetadataDictTest {

    static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    MetadataDict metadataDict;
    Metadata metadata;
    Dict dict;

    @Before
    public void setup() {
        Injector injector = ExampleGuiceInjectors.makeInjector(
                new ValidatorModule(),
                new PropsModule(Lists.newArrayList("tagbase.properties")),
                new MetadataModule()
        );

        MetaStoreConnectorConfig c = injector.getInstance(MetaStoreConnectorConfig.class);

        metadataDict = injector.getInstance(MetadataDict.class);
        metadata = injector.getInstance(Metadata.class);

        dict = iniDict();
    }


    public Dict iniDict() {

        Dict dict = new Dict();
        dict.setVersion("1.0.0");
        dict.setType(DictType.FORWARD);
        dict.setElementCount(500000000);
        dict.setStatus(DictStatus.READY);
        dict.setLocation("/hive/osql/dict/forward1");
        dict.setCreateDate(DateUtil.toLocalDateTime("2020-02-10 10:12:05"));

        return dict;
    }

    /*------------ Start to test --------------*/

    public void addDict() {

        metadataDict.addDict(dict);
        Assert.assertEquals(dict.toString(), metadataDict.getDict().toString());

    }


    public void getDict() {

        metadataDict.addDict(dict);
        Assert.assertEquals(dict.getLocation(), metadataDict.getDict().getLocation());
    }

    public void getDictElementCount() {

        metadataDict.addDict(dict);
        Assert.assertEquals(dict.getElementCount(), metadataDict.getDictElementCount());
    }
}