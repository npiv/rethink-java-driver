package com.rethinkdb;

import java.util.*;

import com.rethinkdb.proto.Q2L.Response;
import com.rethinkdb.proto.Q2L.Response.ResponseType;
import com.rethinkdb.response.DBResultFactory;


public class Cursor<T> implements Iterator<T> {

    private List<T> currentBatch;
    private RethinkDBConnection connection;
    private Response response;
    public final long token;
    private boolean closed;
    private boolean ended;

    public final boolean isFeed;

    public Cursor(RethinkDBConnection connection, Response response) {
        this.connection = connection;
        this.response = response;
        this.currentBatch = (List<T>) DBResultFactory.convert(response);
        this.isFeed = response.getType() == ResponseType.SUCCESS_FEED;
        this.token = response.getToken();
    }

    public boolean isClosed() {
        closed = closed
                || connection.isClosed()
                || response.getType() != ResponseType.SUCCESS_PARTIAL
                && response.getType() != ResponseType.SUCCESS_FEED;
        return closed;
    }

    protected long getToken() {
        return response.getToken();
    }

    @Override
    public boolean hasNext() {
        if (currentBatch.size() > 0) {
            return true;
        } else if (!isClosed()) {
            if (isFeed) {
                return true;
            } else if (response.getType() == ResponseType.SUCCESS_PARTIAL) {
                loadNextBatch();
                return hasNext();
            }
        }
        return false;
    }

    private void loadNextBatch() {
        response = connection.getNext(response.getToken());

        ended = response.getType() != ResponseType.SUCCESS_FEED &&
                response.getType() != ResponseType.SUCCESS_PARTIAL;
        System.out.println(response.getType().toString() + ":" + response.getToken());
        Object converted = DBResultFactory.convert(response);
        currentBatch.addAll((java.util.Collection<T>) converted);
    }

    public void close() {
        if(!isClosed()
                && (response.getType() == ResponseType.SUCCESS_FEED
                || response.getType() == ResponseType.SUCCESS_PARTIAL)) {
            connection.closeCursor(this);
            closed = true;
        }
    }

    private T _next(){
        return currentBatch.remove(0);
    }

    @Override
    public T next() {
        if (currentBatch.size() > 0) {
            return _next();
        } else if (!isClosed()){
            while(!ended && currentBatch.size() == 0){
                loadNextBatch();
            }
            if(currentBatch.size() != 0) {
                return _next();
            }
        }
        throw new NoSuchElementException("There are no more elements to get");
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
