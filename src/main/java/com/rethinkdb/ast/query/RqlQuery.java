package com.rethinkdb.ast.query;

import com.rethinkdb.RethinkDBConnection;
import com.rethinkdb.ast.helper.Arguments;
import com.rethinkdb.ast.helper.OptionalArguments;
import com.rethinkdb.model.Durability;
import com.rethinkdb.model.RqlFunction;
import com.rethinkdb.ast.query.gen.*;
import com.rethinkdb.model.RqlFunction2;
import com.rethinkdb.proto.Q2L;
import com.rethinkdb.response.GroupedResponseConverter;
import sun.security.krb5.internal.crypto.Des;

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

        init(previous, args, optionalArgs);
    }

    protected void init(RqlQuery previous, List<Object> args, Map<String, Object> optionalArgs) {
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

    public <K,V> Map<K,V> runForGroup(RethinkDBConnection connection) {
        return GroupedResponseConverter.convert((Map<String, Object>) run(connection));
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

    public Update update(Map<String, Object> object, Boolean nonAtomic, Durability durability, Boolean returnVals) {
        return new Update(this, new Arguments(RqlUtil.funcWrap(object)),
                new OptionalArguments()
                        .with("non_atomic", nonAtomic)
                        .with("durability", durability)
                        .with("return_vals", returnVals)
        );
    }

    public Update update(Map<String, Object> object) {
        return update(object, null, null, null);
    }

    public Update update(RqlFunction function, Boolean nonAtomic, Durability durability, Boolean returnVals) {
        return new Update(this, new Arguments(RqlUtil.funcWrap(function)),
                new OptionalArguments()
                        .with("non_atomic", nonAtomic)
                        .with("durability", durability)
                        .with("return_vals", returnVals)
        );
    }

    public Update update(RqlFunction function) {
        return update(function, null, null, null);
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

    public ConcatMap concatMap(RqlFunction function) {
        return new ConcatMap(this, new Arguments(RqlUtil.funcWrap(function)), null);
    }

    public OrderBy orderBy(RqlFunction function) {
        return orderBy(null, function);
    }

    public OrderBy orderBy(List<Object> fields, String index) {
        return orderBy(index, fields);
    }

    public OrderBy orderBy(Object... fields) {
        return orderBy(null, fields);
    }

    public OrderBy orderByIndex(String index) {
        return orderBy(index, null);
    }

    public OrderBy orderByField(String field) {
        return orderBy(null, field);
    }

    private OrderBy orderBy(String index, Object... fields) {
        List<Object> args = new ArrayList<Object>();
        for (Object field : fields) {
            if (field instanceof Asc || field instanceof Desc) {
                args.add(field);
            }
            else {
                args.add(RqlUtil.funcWrap(field));
            }
        }
        return new OrderBy(this, new Arguments(args), new OptionalArguments().with("index", index));
    }

    public Get get(String key) {
        return new Get(this, new Arguments(key), null);
    }

    public GetAll get(List<String> keys) {
        return get(keys, null);
    }

    public GetAll get(List<String> keys, String index) {
        return new GetAll(this, new Arguments(keys), new OptionalArguments().with("index", index));
    }

    public Filter filter(RqlFunction function) { return new Filter(this, new Arguments(new Func(function)), null); }

    public Filter filter(RqlQuery query) { return new Filter(this, new Arguments(query), null); }

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

    public Branch branch(RqlQuery predicate, Map<String,Object> trueBranch, Map<String,Object> falseBranch) {
        return new Branch(predicate, new Arguments(trueBranch, falseBranch), null);
    }

    public Delete delete() {
        return delete(null, null);
    }

    public Delete delete(Durability durability, Boolean withVals) {
        return new Delete(this, null, new OptionalArguments().with("durability", durability).with("with_vals", withVals));
    }

    public Count count() {
        return new Count(this, null, null);
    }

    public Sync sync() {
        return new Sync(this, null, null);
    }

    public Match match(String regexp) {
        return new Match(this, new Arguments(regexp), null);
    }

    public RqlQuery expr(Object expr) {
        return RqlUtil.toRqlQuery(expr);
    }

    public WithFields withFields(String... fields) {
        return new WithFields(this, new Arguments(fields), null);
    }

    public InnerJoin innerJoin(RqlQuery other, RqlFunction2 predicate) {
        return new InnerJoin(this, new Arguments(other, RqlUtil.funcWrap(predicate)), null);
    }

    public OuterJoin outerJoin(RqlQuery other, RqlFunction2 predicate) {
        return new OuterJoin(this, new Arguments(other, RqlUtil.funcWrap(predicate)), null);
    }

    public EqJoin eqJoin(String leftAttribute, Table otherTable) {
        return eqJoin(leftAttribute, otherTable, null);
    }

    public EqJoin eqJoin(String leftAttribute, Table otherTable, String indexId) {
        return new EqJoin(this, new Arguments(leftAttribute, otherTable), new OptionalArguments().with("index",indexId));
    }

    public EqJoin eqJoin(RqlFunction leftAttribute, Table otherTable) {
        return eqJoin(leftAttribute, otherTable, null);
    }

    public EqJoin eqJoin(RqlFunction leftAttribute, Table otherTable, String indexId) {
        return new EqJoin(this, new Arguments(RqlUtil.funcWrap(leftAttribute), otherTable), new OptionalArguments().with("index",indexId));
    }

    public Zip zip() {
        return new Zip(this, null, null);
    }


    public Desc desc(String key) {
        return new Desc(this, new Arguments(key), null);
    }

    public Asc asc(String key) {
        return new Asc(this, new Arguments(key), null);
    }

    public Skip skip(int n) {
        return new Skip(this, new Arguments(n), null);
    }

    public Limit limit(int n) {
        return new Limit(this, new Arguments(n), null);
    }

    public IndexesOf indexesOf(Object value) {
        return new IndexesOf(this, new Arguments(value), null);
    }

    public IndexesOf indexesOf(RqlQuery predicate) {
        return new IndexesOf(this, new Arguments(RqlUtil.funcWrap(predicate)), null);
    }

    public IsEmpty isEmpty() {
        return new IsEmpty(this, null, null);
    }

    public Union union(RqlQuery other) {
        return new Union(this, new Arguments(other), null);
    }

    public Sample sample(int size) {
        return new Sample(this, new Arguments(size), null);
    }

    public Max max(String field) {
        return new Max(this, new Arguments(field), null);
    }

    public Max max(RqlFunction func) {
        return new Max(this, new Arguments(RqlUtil.funcWrap(func)), null);
    }

    public Min min(String field) {
        return new Min(this, new Arguments(field), null);
    }

    public Min min(RqlFunction func) {
        return new Min(this, new Arguments(RqlUtil.funcWrap(func)), null);
    }

    public Group group(RqlFunction func) {
        return group(func, null);
    }
    public Group group(RqlFunction func, String index) {
        return new Group(this, new Arguments(RqlUtil.funcWrap(func)), new OptionalArguments().with("index",index));
    }
    public Group group(String field) {
        return group(field, null);
    }
    public Group group(String field, List index) {
        return new Group(this, new Arguments(field), new OptionalArguments().with("index",index));
    }

    public Ungroup ungroup() {
        return new Ungroup(this, null, null);
    }

    public Reduce reduce(RqlFunction2 func) {
        return new Reduce(this, new Arguments(RqlUtil.funcWrap(func)), null);
    }

    public Sum sum() {
        return new Sum(this, null, null);
    }

    public Avg avg() {
        return new Avg(this, null, null);
    }

    public Min min() {
        return new Min(this, null, null);
    }

    public Max max() {
        return new Max(this, null, null);
    }

    public Distinct distinct() {
        return new Distinct(this, null, null);
    }

    public Contains contains(List<Object> objects) {
        List<Object> args = new ArrayList<Object>();
        for (Object object : objects) {
            args.add(RqlUtil.funcWrap(object));
        }
        return new Contains(this, args, null);
    }
}
