package com.rethinkdb.query;

import com.rethinkdb.proto.Q2L;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Used within the query package to store and build a ProtoBuf query. This class is
 * never exposed as an external api and is only used to facilitate the RethinkQueryBuilder
 * within the query package.
 */
class QueryInformation {
    private Q2L.Query.QueryType queryType = Q2L.Query.QueryType.START;
    private Q2L.Term.TermType termType;
    private List<Q2L.Term> args = new ArrayList<Q2L.Term>();
    private List<Q2L.Term.AssocPair> optsArgs = new ArrayList<Q2L.Term.AssocPair>();

    /**
     * Set the query type
     * @param type query type
     * @return self
     */
    public QueryInformation ofQueryType(Q2L.Query.QueryType type) {
        this.queryType = type;
        return this;
    }

    /**
     * Set the termType of the first inner Term of the query. This corresponds to the action,
     * INSERT, DB_LIST etc..
     *
     * @param termType term type
     * @return self
     */
    public QueryInformation ofTermType(Q2L.Term.TermType termType) {
        this.termType = termType;
        return this;
    }

    /**
     * The args of the first inner Term of the query. These correspond to the parameters of the
     * term type
     * @param args Term args
     * @return self
     */
    public QueryInformation withArgs(Q2L.Term... args) {
        this.args.addAll(Arrays.asList(args));
        return this;
    }

    public QueryInformation withArg(Q2L.Term arg) {
        this.args.add(arg);
        return this;
    }

    /**
     * Add an optional args of the first inner Term of the query.
     * @param name arg name
     * @param value arg value
     * @return self
     */
    public QueryInformation withOptArg(String name, Q2L.Term value) {
        this.optsArgs.add(Q2L.Term.AssocPair.newBuilder().setKey(name).setVal(value).build());
        return this;
    }

    protected Q2L.Query.QueryType getQueryType() {
        return queryType;
    }

    protected Q2L.Term.TermType getTermType() {
        return termType;
    }

    protected List<Q2L.Term> getArgs() {
        return args;
    }

    protected List<Q2L.Term.AssocPair> getOptsArgs() {
        return optsArgs;
    }

}
