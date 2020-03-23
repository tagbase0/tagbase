package com.oppo.tagbase.common.guice;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.oppo.tagbase.dict.util.AnnotationUtil;

import javax.validation.Validator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Bind configuration prefix to get a config instance.
 * <p>
 * For example : If you have some configuration items with prefix "tagbase.storage" and
 * a config class HdfsStorageConf.class
 * <p>
 * Invoking ConfBind.bind("tagbase.storage", HdfsStorageConf.class) will bind HdfsStorageConf instance
 * into Guice. So you can inject the config instance into other object.
 * <p>
 * Created by wujianchao on 2020/1/20.
 */
public class ConfBind {


    public static <T> void bind(Binder binder, Class<T> configClazz) {
        String confKeyPrefix = AnnotationUtil.getAnnotation(configClazz, Config.class).value();
        bind(binder, confKeyPrefix, configClazz);
    }

    @Deprecated
    public static <T> void bind(Binder binder, String confKeyPrefix, Class<T> configClazz) {
        binder.bind(configClazz).toProvider(new ConfigProvider<T>(confKeyPrefix, configClazz)).in(Scopes.SINGLETON);
    }

    public static <T> void bind(Binder binder, Class<T> configClazz, String name) {
        String confKeyPrefix = AnnotationUtil.getAnnotation(configClazz, Config.class).value();
        bind(binder, confKeyPrefix, configClazz, name);
    }

    @Deprecated
    public static <T> void bind(Binder binder, String confKeyPrefix, Class<T> configClazz, String name) {
        binder.bind(configClazz).annotatedWith(Names.named(name)).toProvider(new ConfigProvider<T>(confKeyPrefix, configClazz)).in(Scopes.SINGLETON);
    }

    static class ConfigProvider<T> implements Provider {

        private final String confKeyPrefix;
        private final Class<T> configClazz;

        ConfigProvider(String confKeyPrefix, Class<T> configClazz) {
            this.confKeyPrefix = confKeyPrefix;
            this.configClazz = configClazz;
        }

        private Properties props;
        private ObjectMapper om;
        private Validator validator;

        @Inject
        private void inject(Properties props, ObjectMapper om, Validator validator) {
            this.om = om;
            this.props = props;
            this.validator = validator;
        }

        @Override
        public T get() {

            Map<String, String> configEntries = props.keySet().stream()
                    .map(Object::toString)
                    .filter(key -> key.startsWith(confKeyPrefix))
                    .collect(Collectors.toMap(
                            key -> key.substring(confKeyPrefix.length() + 1),
                            props::getProperty)
                    );

            T conf = om.convertValue(configEntries, configClazz);

            validate(conf);

            return conf;
        }

        private void validate(T conf) {
//            Set<ConstraintViolation<T>> validations = validator.validate(conf);
//            if (!validations.isEmpty()) {
//                List<String> messages = new ArrayList<>();
//
//                for (ConstraintViolation<T> violation : validations) {
//                    StringBuilder path = new StringBuilder();
//                    try {
//                        Class<?> beanClazz = violation.getRootBeanClass();
//                        final Iterator<Path.Node> iterator = violation.getPropertyPath().iterator();
//                        while (iterator.hasNext()) {
//                            Path.Node next = iterator.next();
//                            if (next.getKind() == ElementKind.PROPERTY) {
//                                final String fieldName = next.getName();
//                                final Field theField = beanClazz.getDeclaredField(fieldName);
//
//                                if (theField.getAnnotation(JacksonInject.class) != null) {
//                                    path = new StringBuilder(String.format(" -- Injected field[%s] not bound!?", fieldName));
//                                    break;
//                                }
//
//                                JsonProperty annotation = theField.getAnnotation(JsonProperty.class);
//                                final boolean noAnnotationValue = annotation == null || Strings.isNullOrEmpty(annotation.value());
//                                final String pathPart = noAnnotationValue ? fieldName : annotation.value();
//                                if (path.length() == 0) {
//                                    path.append(pathPart);
//                                } else {
//                                    path.append(".").append(pathPart);
//                                }
//                            }
//                        }
//                    } catch (NoSuchFieldException e) {
//                        throw new RuntimeException(e);
//                    }
//
//                    messages.add(String.format("%s - %s", path.toString(), violation.getMessage()));
//                }
//
//                throw new ProvisionException(
//                        Iterables.transform(
//                                messages,
//                                (input) -> new Message(String.format("%s.%s", confKeyPrefix, input))
//                        )
//                );
//            }
        }
    }
}