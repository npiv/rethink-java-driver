package com.rethinkdb.ast.query.gen;

import com.rethinkdb.ast.helper.Arguments;
import com.rethinkdb.model.RqlFunction;
import com.rethinkdb.ast.query.RqlQuery;
import com.rethinkdb.ast.query.RqlUtil;
import com.rethinkdb.model.RqlFunction2;
import com.rethinkdb.model.RqlLambda;
import com.rethinkdb.proto.Q2L;

// extends RqlQuery
public class Func extends RqlQuery {

    public Func(RqlLambda function) {
        super(Q2L.Term.TermType.FUNC, null, null);

        if (function instanceof RqlFunction) {
            super.init(
                    null,
                    new Arguments(
                            new MakeArray(new Arguments(1), null),
                            RqlUtil.toRqlQuery(((RqlFunction)function).apply(new Var(new Arguments(1), null)))
                    ),
                    null
            );
        }
        else {
            super.init(
                    null,
                    new Arguments(
                            new MakeArray(new Arguments(1,2), null),
                            RqlUtil.toRqlQuery(((RqlFunction2)function).apply(new Var(new Arguments(1), null), new Var(new Arguments(2), null)))
                    ),
                    null
            );
        }
    }
}
        