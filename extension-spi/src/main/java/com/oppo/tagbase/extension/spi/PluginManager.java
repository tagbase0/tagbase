package com.oppo.tagbase.extension.spi;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * @author huangfeng
 * @date 2020/2/28 14:12
 */
public class PluginManager {
    private static final List<String> SPI_PACKAGES = Arrays.asList("com.oppo.tagbase.extension.spi");

    Map<String, ResultWriterFactory> resultWriterFactories = new HashMap<>();

    public void register(String name, ResultWriterFactory factory) {
        resultWriterFactories.put(name, factory);
    }

    public void load(String dirPath) throws Exception {

        URLClassLoader classLoader = buildExtensionLoader(dirPath);

        ServiceLoader<ResultWriterFactory> writerFactories = ServiceLoader.load(ResultWriterFactory.class, classLoader);
        for (ResultWriterFactory writerFactory : writerFactories) {
            register(writerFactory.getName(), writerFactory);
        }
    }

    private URLClassLoader buildExtensionLoader(String dirPath) throws Exception {
        File dir = new File(dirPath);
        if(dir.isDirectory()){
            return buildClassLoaderFromDirectory(dir);
        }
        throw new RuntimeException("can't load from not dir");
    }

    private URLClassLoader buildClassLoaderFromDirectory(File dir)
            throws Exception {
        List<URL> urls = new ArrayList<>();
        for (File file : listFiles(dir)) {
            urls.add(file.toURI().toURL());
        }
        return createClassLoader(urls);
    }

    private URLClassLoader createClassLoader(List<URL> urls) {
        ClassLoader parent = getClass().getClassLoader();
        return new PluginClassLoader(urls, parent, SPI_PACKAGES);
    }


    private static List<File> listFiles(File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                Arrays.sort(files);
                return Arrays.asList(files);
            }
        }
        return new ArrayList<>();
    }
}
