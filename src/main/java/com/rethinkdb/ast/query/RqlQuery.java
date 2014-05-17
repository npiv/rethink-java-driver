package com.rethinkdb.ast.query;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.Arguments;
import com.rethinkdb.ast.RqlFunction;
import com.rethinkdb.ast.query.gen.*;
import com.rethinkdb.proto.Q2L;

import java.util.*;

public class RqlQuery {

    public static RqlQuery R = new RqlQuery(null, null);

    private Q2L.Term.TermType termType;
    protected List<RqlQuery> args = new ArrayList<RqlQuery>();
    protected java.util.Map<String, RqlQuery> optionalArgs = new HashMap<String, RqlQuery>();

    // ** Protected Methods ** //

    protected RqlQuery(Q2L.Term.TermType termType, List<Object> args) {
        this(termType, args, new HashMap<String, Object>());
    }

    protected RqlQuery(Q2L.Term.TermType termType) {
        this(termType, new ArrayList<Object>(), new HashMap<String, Object>());
    }

    protected RqlQuery(Q2L.Term.TermType termType, List<Object> args, java.util.Map<String, Object> optionalArgs) {
        this(null, termType, args, optionalArgs);
    }

    public RqlQuery(RqlQuery previous, Q2L.Term.TermType termType, List<Object> args, Map<String, Object> optionalArgs) {
        this.termType = termType;

        if (previous != null && previous.termType != null) {
            this.args.add(previous);
        }

        if (args != null) {
            for (Object arg : args) {
                this.args.add(RqlUtil.toRqlQuery(arg));
            }
        }

        if (optionalArgs != null) {
            for (Map.Entry<String, Object> kv : optionalArgs.entrySet()) {
                this.optionalArgs.put(kv.getKey(), RqlUtil.toRqlQuery(kv.getValue()));
            }
        }
    }

    protected Q2L.Term.TermType getTermType() {
        return termType;
    }

    protected List<RqlQuery> getArgs() {
        return args;
    }

    protected Map<String, RqlQuery> getOptionalArgs() {
        return optionalArgs;
    }

    protected Q2L.Term toTerm() {
        Q2L.Term.Builder termBuilder = Q2L.Term.newBuilder().setType(termType);
        for (RqlQuery arg : args) {
            termBuilder.addArgs(arg.toTerm());
        }
        for (Map.Entry<String, RqlQuery> kv : optionalArgs.entrySet()) {
            termBuilder.addOptargs(
                    Q2L.Term.AssocPair.newBuilder()
                            .setKey(kv.getKey())
                            .setVal(kv.getValue().toTerm())
            ).build();
        }
        return termBuilder.build();
    }

    // ** Public API **//

    public Object run(RethinkDBConnection connection) {
        return connection.run(toTerm());
    }

    public DbCreate dbCreate(String dbName) {
        return new DbCreate(new Arguments(dbName), null);
    }

    public DbDrop dbDrop(String dbName) {
        return new DbDrop(new Arguments(dbName), null);
    }

    public DbList dbList() {
        return new DbList(null, null);
    }

    public RqlQuery eq(Object... queries) {
        return new Eq(this,Arrays.asList(queries), null);
    }

    public RqlQuery ne(Object... queries) {
        return new Ne(this,Arrays.asList(queries), null);
    }

    public RqlQuery lt(Object... queries) {
        return new Lt(this,Arrays.asList(queries), null);
    }

    public RqlQuery le(Object... queries) {
        return new Le(this,Arrays.asList(queries), null);
    }

    public RqlQuery gt(Object... queries) {
        return new Gt(this,Arrays.asList(queries), null);
    }

    public RqlQuery ge(Object... queries) {
        return new Ge(this,Arrays.asList(queries), null);
    }

    public RqlQuery add(Object... queries) {
        return new Add(this, Arrays.asList(queries), null);
    }

    public RqlQuery sub(Object... queries) {
        return new Sub(this, Arrays.asList(queries), null);
    }

    public RqlQuery mul(Object... queries) {
        return new Mul(this,Arrays.asList(queries), null);
    }

    public RqlQuery div(Object... queries) {
        return new Div(this,Arrays.asList(queries), null);
    }

    public RqlQuery mod(Object... queries) {
        return new Mod(this,Arrays.asList(queries), null);
    }

    public RqlQuery and(Object... queries) {
        return new All(this,Arrays.asList(queries), null);
    }

    public RqlQuery or(Object... queries) {
        return new Any(this,Arrays.asList(queries), null);
    }

    public RqlQuery not(Object... queries) {
        return new Not(this,Arrays.asList(queries), null);
    }

    public DB db(String db) {
        return new DB(new Arguments(db), null);
    }

    public Update update(Map<String, Object> object) {
        return new Update(this, new Arguments(RqlUtil.funcWrap(object)), null);
    }

    public ImplicitVar row() {
        return new ImplicitVar(null, null, null);
    }

    public GetField field(String field) {
        return new GetField(this, new Arguments(field), null);
    }

    public RMap map(RqlFunction function) {
        return new RMap(this, new Arguments(new Func(function)), null);
    }

    public Filter filter(RqlFunction function) { return new Filter(this, new Arguments(new Func(function)), null); }

    public Table table(String tableName) {
        return new Table(this, new Arguments(tableName), null);
    }

    public Upcase upcase() {
        return new Upcase(this, null, null);
    }

    public Downcase downcase() {
        return new Downcase(this, null, null);
    }

    public Split split(String s) {
        return new Split(this, new Arguments(s), null);
    }

    public Replace replace(Map<String, Object> replacement) {
        return new Replace(this, new Arguments(replacement), null);
    }

    public Replace replace(RqlFunction function) {
        return new Replace(this, new Arguments(new Func(function)), null);
    }

    public RqlQuery without(String field) {
        return new Without(this, new Arguments(field), null);
    }

    public Pluck pluck(List<String> fields) {
        return new Pluck(this, new Arguments(fields), null);
    }
}
