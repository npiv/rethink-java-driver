package com.rethinkdb.ast;

import com.rethinkdb.proto.Q2L;

import java.util.*;

public class RTOperation {
    private Q2L.Term.TermType termType;
    private List<Object> args = new ArrayList<Object>();
    private Map<String, Object> optionalArgs = new HashMap<String, Object>();

    public RTOperation(Q2L.Term.TermType termType) {
        this.termType = termType;
    }

    protected RTOperation pushArg(Object arg) {
        this.args.add(0, arg);
        return this;
    }

    public RTOperation withArgs(Object... args) {
        this.args.addAll(Arrays.asList(args));
        return this;
    }

    public RTOperation withOptionalArg(String key, Object value) {
        this.optionalArgs.put(key,value);
        return this;
    }

    public Q2L.Term.TermType getTermType() {
        return termType;
    }

    public List<Object> getArgs() {
        return args;
    }

    public Map<String, Object> getOptionalArgs() {
        return optionalArgs;
    }

    public static RTOperation db(String dbname) {
        return new RTOperation(Q2L.Term.TermType.DB).withArgs(dbname);
    }

    @Override
    public String toString() {
        return "RTOperation{" +
                "termType=" + termType +
                ", args=" + args +
                ", optionalArgs=" + optionalArgs +
                "}";
    }
}
