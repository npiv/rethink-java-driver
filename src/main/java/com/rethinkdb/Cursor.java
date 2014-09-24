package com.rethinkdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class Cursor<T> implements Iterator<T> {

    private enum State {
        EMPTY, HAS_MORE, DONE
    }

    private List<T> currentBatch;
    private RethinkDBConnection connection;
    private long token;
    private State state;

    public Cursor(List<T> currentBatch, RethinkDBConnection connection, long token, boolean hasMore) {
        this.currentBatch = currentBatch;
        this.connection = connection;
        this.token = token;
        this.state = hasMore ? State.HAS_MORE : State.DONE;
    }

    @Override
    public boolean hasNext() {
        if (currentBatch != null && currentBatch.size() > 0) {
            return true;
        }

        switch (state) {
            case DONE: return false;
            default:
                loadNextBatch();
                return hasNext();
        }
    }

    private void loadNextBatch() {
        currentBatch.addAll(connection.<java.util.Collection<T>>getNext(token));
    }

    @Override
    public T next() {
        if (currentBatch != null && currentBatch.size() > 0) {
            return currentBatch.remove(0);
        }

        throw new NoSuchElementException("There are no more elements to get");
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
