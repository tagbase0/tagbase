package com.oppo.tagbase.guice;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.inject.*;
import com.google.inject.spi.Message;

import javax.validation.ConstraintViolation;
import javax.validation.ElementKind;
import javax.validation.Path;
import javax.validation.Validator;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Bind configuration prefix to get a config instance.
 * <p>
 * For example : If you have some configuration items with prefix "tagbase.storage" and
 * a config class StorageConf.class
 * <p>
 * Invoking ConfBind.bind("tagbase.storage", StorageConf.class) will bind StorageConf instance
 * into Guice.
 * <p>
 * Created by wujianchao on 2020/1/20.
 */
public class ConfBind {


    public static <T> void bind(Binder binder, String confKeyPrefix, Class<T> configClazz) {
        binder.bind(configClazz).toProvider(new ConfigProvider<T>(confKeyPrefix, configClazz)).in(Scopes.SINGLETON);
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
            Set<ConstraintViolation<T>> validations = validator.validate(conf);
            if (!validations.isEmpty()) {
                List<String> messages = new ArrayList<>();

                for (ConstraintViolation<T> violation : validations) {
                    StringBuilder path = new StringBuilder();
                    try {
                        Class<?> beanClazz = violation.getRootBeanClass();
                        final Iterator<Path.Node> iterator = violation.getPropertyPath().iterator();
                        while (iterator.hasNext()) {
                            Path.Node next = iterator.next();
                            if (next.getKind() == ElementKind.PROPERTY) {
                                final String fieldName = next.getName();
                                final Field theField = beanClazz.getDeclaredField(fieldName);

                                if (theField.getAnnotation(JacksonInject.class) != null) {
                                    path = new StringBuilder(String.format(" -- Injected field[%s] not bound!?", fieldName));
                                    break;
                                }

                                JsonProperty annotation = theField.getAnnotation(JsonProperty.class);
                                final boolean noAnnotationValue = annotation == null || Strings.isNullOrEmpty(annotation.value());
                                final String pathPart = noAnnotationValue ? fieldName : annotation.value();
                                if (path.length() == 0) {
                                    path.append(pathPart);
                                } else {
                                    path.append(".").append(pathPart);
                                }
                            }
                        }
                    } catch (NoSuchFieldException e) {
                        throw new RuntimeException(e);
                    }

                    messages.add(String.format("%s - %s", path.toString(), violation.getMessage()));
                }

                throw new ProvisionException(
                        Iterables.transform(
                                messages,
                                (input) -> new Message(String.format("%s%s", confKeyPrefix, input))
                        )
                );
            }
        }
    }
}