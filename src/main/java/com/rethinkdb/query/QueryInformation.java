package com.rethinkdb.query;

import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.DBResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryInformation {
    private Q2L.Query.QueryType queryType = Q2L.Query.QueryType.START;
    private Q2L.Term.TermType termType;
    private List<Q2L.Term> args;
    private List<Q2L.Term.AssocPair> optsArgs = new ArrayList<Q2L.Term.AssocPair>();

    public QueryInformation ofQueryType(Q2L.Query.QueryType type) {
        this.queryType = type;
        return this;
    }

    public QueryInformation ofTermType(Q2L.Term.TermType termType) {
        this.termType = termType;
        return this;
    }

    public QueryInformation withArgs(Q2L.Term... args) {
        this.args = Arrays.asList(args);
        return this;
    }

    public QueryInformation withOptArg(String name, Q2L.Term value) {
        this.optsArgs.add(Q2L.Term.AssocPair.newBuilder().setKey(name).setVal(value).build());
        return this;
    }

    public Q2L.Query.QueryType getQueryType() {
        return queryType;
    }

    public Q2L.Term.TermType getTermType() {
        return termType;
    }

    public List<Q2L.Term> getArgs() {
        return args;
    }

    public List<Q2L.Term.AssocPair> getOptsArgs() {
        return optsArgs;
    }

}
