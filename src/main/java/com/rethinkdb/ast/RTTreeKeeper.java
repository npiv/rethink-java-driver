package com.rethinkdb.ast;

import com.rethinkdb.proto.Q2L;

public class RTTreeKeeper {
    private RTOperation tree = null;

    public RTTreeKeeper() {
    }

    private RTTreeKeeper(RTOperation data) {
        this.tree = data;
    }

    public RTTreeKeeper addData(RTOperation data) {
        if (this.tree == null) {
            return new RTTreeKeeper(data);
        }
        else {
            data.pushArg(this.tree);
            return new RTTreeKeeper(data);
        }
    }

    public RTOperation getTree() {
        return tree;
    }
}
