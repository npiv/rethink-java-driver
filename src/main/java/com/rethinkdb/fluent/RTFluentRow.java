package com.rethinkdb.fluent;

import com.rethinkdb.ast.RTOperation;
import com.rethinkdb.ast.RTTreeKeeper;
import com.rethinkdb.proto.Q2L;

public class RTFluentRow extends RTTopLevelQuery {

    public RTFluentRow() {
    }

    public RTFluentRow(RTTreeKeeper treeKeeper, Class sampleClass) {
        super(treeKeeper, sampleClass);
    }

    private RTFluentRow makeLambdaOperation(Q2L.Term.TermType type, Object arg) {
        RTOperation operation = new RTOperation(type)
                .withArgs(new RTOperation(Q2L.Term.TermType.VAR).withArgs(1))
                .withArgs(arg);

        return new RTFluentRow(treeKeeper.addData(operation), Object.class);
    }

    private RTFluentRow makeOperation(Q2L.Term.TermType type, Object... arg) {
        RTOperation operation = new RTOperation(type).withArgs(arg);
        return new RTFluentRow(treeKeeper.addData(operation), Object.class);
    }

    public RTFluentRow field(String fieldName) {
        return makeLambdaOperation(Q2L.Term.TermType.GET_FIELD, fieldName);
    }

    public RTFluentRow without(String fieldName) {
        return makeLambdaOperation(Q2L.Term.TermType.WITHOUT, fieldName);
    }

    public RTFluentRow add(double op) {
        return makeOperation(Q2L.Term.TermType.ADD, op);
    }
    public RTFluentRow add(String op) {
        return makeOperation(Q2L.Term.TermType.ADD, op);
    }
    public RTFluentRow subtract(double op) {
        return makeOperation(Q2L.Term.TermType.SUB, op);
    }
    public RTFluentRow multiply(double op) {
        return makeOperation(Q2L.Term.TermType.MUL, op);
    }
    public RTFluentRow divide(double op) {
        return makeOperation(Q2L.Term.TermType.DIV, op);
    }
    public RTFluentRow modulus(double op) {
        return makeOperation(Q2L.Term.TermType.MOD, op);
    }
    public RTFluentRow and(double op) {
        return makeOperation(Q2L.Term.TermType.MOD, op);
    }

    public RTFluentRow lt(Object... o) {
        return makeOperation(Q2L.Term.TermType.LT, o);
    }
    public RTFluentRow gt(Object... o) {
        return makeOperation(Q2L.Term.TermType.GT, o);
    }
    public RTFluentRow le(Object... o) {
        return makeOperation(Q2L.Term.TermType.LE, o);
    }
    public RTFluentRow ge(Object... o) {
        return makeOperation(Q2L.Term.TermType.GE, o);
    }
    public RTFluentRow eq(Object... o) {
        return makeOperation(Q2L.Term.TermType.EQ, o);
    }
    public RTFluentRow ne(Object... o) {
        return makeOperation(Q2L.Term.TermType.NE, o);
    }
    public RTFluentRow not(Object... o) {
        return makeOperation(Q2L.Term.TermType.NOT, o);
    }

    public RTFluentRow all(Object... o) {
        return makeOperation(Q2L.Term.TermType.ALL, o);
    }
    public RTFluentRow and(Object... o) {
        return makeOperation(Q2L.Term.TermType.ALL, o);
    }

    public RTFluentRow any(Object... o) {
        return makeOperation(Q2L.Term.TermType.ANY, o);
    }
    public RTFluentRow or(Object... o) {
        return makeOperation(Q2L.Term.TermType.ANY, o);
    }

//    public RTFluentRow and(RTFluentRow o, RTFluentRow o2) {
//        RTOperation operation = new RTOperation(Q2L.Term.TermType.ALL).withArgs(
//                o.treeKeeper.getTree(),
//                o2.treeKeeper.getTree()
//        );
//
//        return new RTFluentRow(treeKeeper.addData(operation), Object.class);
//    }
//
//
//    public RTFluentRow or(RTFluentRow o, RTFluentRow o2) {
//        RTOperation operation = new RTOperation(Q2L.Term.TermType.ANY).withArgs(
//                o.treeKeeper.getTree(),
//                o2.treeKeeper.getTree()
//        );
//
//        return new RTFluentRow(treeKeeper.addData(operation), Object.class);
//    }
}
