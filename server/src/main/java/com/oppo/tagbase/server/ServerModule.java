package com.oppo.tagbase.server;


import com.google.inject.Binder;
import com.google.inject.Module;

public class ServerModule implements Module {
    @Override
    public void configure(Binder binder) {
        binder.bind(Server.class);

    }
}
