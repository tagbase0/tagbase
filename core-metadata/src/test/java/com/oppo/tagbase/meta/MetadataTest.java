package com.oppo.tagbase.meta;

import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.ExampleGuiceInjectors;
import com.oppo.tagbase.common.guice.PropsModule;
import com.oppo.tagbase.common.guice.ValidatorModule;
import com.oppo.tagbase.meta.connector.MetaStoreConnectorConfig;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by wujianchao on 2020/2/27.
 */
public class MetadataTest {

    Metadata metadata;

    @Before
    public void setup() {
        Injector injector = ExampleGuiceInjectors.makeInjector(
                new ValidatorModule(),
                new PropsModule(Lists.newArrayList("tagbase.properties")),
                new MetadataModule()
        );

        MetaStoreConnectorConfig c = injector.getInstance(MetaStoreConnectorConfig.class);

        metadata =injector.getInstance(Metadata.class);
    }

    @Test
    public void initSchemaTest() {
        metadata.initSchema();
    }
}
