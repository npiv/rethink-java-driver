package com.rethinkdb.ast;

import com.rethinkdb.fluent.Lambda;
import com.rethinkdb.fluent.RTFluentQuery;
import com.rethinkdb.fluent.RTFluentRow;
import com.rethinkdb.model.DBLambda;
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

    public RTOperation withArgs(List<Object> args) {
        for (Object arg : args) {
            if (arg instanceof RTFluentRow) {
                this.args.add(((RTFluentRow)arg).treeKeeper.getTree());
            }
            else {
                this.args.add(arg);
            }
        }
        return this;
    }

    public RTOperation withArgs(Object... args) {
        return withArgs(Arrays.asList(args));
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

    public static RTOperation lambda(Q2L.Term.TermType type, DBLambda lambda) {
        return new RTOperation(type)
                .withArgs(RTLambdaConverter.getOperation(lambda));
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
