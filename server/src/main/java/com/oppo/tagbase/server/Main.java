package com.oppo.tagbase.server;


import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) {
        try {
            Injector injector = GuiceInjectors.makeStartupInjector();
            injector.getInstance(Server.class).run();
        } catch (Exception e) {
            LOG.error("startup error", e);
        }
    }
}
