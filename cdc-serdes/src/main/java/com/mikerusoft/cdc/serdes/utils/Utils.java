package com.mikerusoft.cdc.serdes.utils;

import com.mikerusoft.cdc.serdes.model.SectionKey;

public class Utils {
    public static String generateKey(SectionKey section) {
        return section.getSectionId() + "|" + section.getDyid() + "|" + section.getRri();
    }

    public static <T> T rethrowRuntime(Throwable t) {
        if (t instanceof Error)
            throw (Error)t;
        else if (t instanceof RuntimeException)
            throw (RuntimeException)t;
        else throw new RuntimeException(t);
    }

    public static void rethrow(Throwable t) {
        rethrowRuntime(t);
    }

}
