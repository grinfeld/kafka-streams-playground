package com.mikerusoft.playground.udhi;

import com.mikerusoft.playground.models.udhi.GroupMessage;
import com.mikerusoft.playground.models.udhi.UdhiMessage;

public class Utils {

    public static GroupMessage addMessageToGroup(UdhiMessage v, GroupMessage a) {
        if (a.getSize() == 0) { a.setSize(v.getSize());}
        a.add(v);
        return a;
    }
}
