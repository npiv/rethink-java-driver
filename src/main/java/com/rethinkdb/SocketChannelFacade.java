package com.rethinkdb;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rethinkdb.proto.Q2L;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class SocketChannelFacade {
    private SocketChannel socketChannel;

    public void connect(String hostname, int port) {
        try {
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            socketChannel.connect(new InetSocketAddress(hostname, port));

        } catch (IOException e) {
            throw new RethinkDBException(e);
        }
    }

    private void _write(ByteBuffer buffer) {
        try {
            buffer.flip();
            while (buffer.hasRemaining()) {
                socketChannel.write(buffer);
            }
        } catch (IOException e) {
            throw new RethinkDBException(e);
        }
    }

    private ByteBuffer _read(int i, boolean strict) {
        ByteBuffer buffer = ByteBuffer.allocate(i);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        try {
            int read = socketChannel.read(buffer);
            if (read != i && strict) {
                throw new RethinkDBException("Error receiving data, expected " + i + " bytes but received " + read);
            }
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            throw new RethinkDBException(e);
        }
    }

    public void writeLEInt(int i) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(i);
        _write(buffer);
    }

    public int readLEInt() {
        ByteBuffer buffer = _read(4, true);
        return buffer.getInt();
    }

    public void writeStringWithLength(String s) {
        writeLEInt(s.length());

        ByteBuffer buffer = ByteBuffer.allocate(s.length());
        buffer.put(s.getBytes());
        _write(buffer);
    }

    public String readString() {
        return new String(_read(5000, false).array());
    }

    public void write(byte[] bytes) {
        writeLEInt(bytes.length);

        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);

        _write(buffer);
    }

    public Q2L.Response read() {
        try {
            int len = readLEInt();
            ByteBuffer buffer = _read(len, true);
            return Q2L.Response.parseFrom(buffer.array());

        } catch (InvalidProtocolBufferException e) {
            throw new RethinkDBException(e);
        }
    }
}
