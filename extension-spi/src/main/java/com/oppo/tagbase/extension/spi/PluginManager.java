package com.oppo.tagbase.extension.spi;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;

/**
 * @author huangfeng
 * @date 2020/2/28 14:12
 */
public class PluginManager {

    private static final List<String> SPI_PACKAGES = Arrays.asList("com.oppo.tagbase.extension.spi","com.google.inject","core-site","hdfs-site");


    public static void main(String[] args) throws Exception {

        List<Module> modules = new PluginManager().load("plugin");
        Injector injector = Guice.createInjector(modules);

        FileSystem fileSystem = injector.getInstance(FileSystem.class);

        if(args[0].equals("1")) {
            fileSystem.copyToLocalFile("/test/feng/example",".");
        }else if(args[0].equals("2")) {
            fileSystem.copyFromLocalFile("a", "/test/feng/");
        }else {
            fileSystem.getFileSize(args[0]);
        }
        System.out.println(fileSystem.getClass());
    }

    public List<Module> load(String dirPath) throws Exception {

        URLClassLoader classLoader = buildExtensionLoader(dirPath);

        ServiceLoader<Module> pluginModules = ServiceLoader.load(Module.class, classLoader);

        return ImmutableList.copyOf(pluginModules);
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
