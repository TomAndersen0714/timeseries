package cn.tomandersen.timeseries.compression;

import java.nio.ByteBuffer;

/**
 * <h3>BitBuffer</h3>
 * A extension of {@link ByteBuffer} for accessing byte buffer by bit.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/30
 */
public abstract class BitBuffer {

    // Byte buffer for reader/writer
    protected ByteBuffer buffer;
    // Cached byte for reader.
    protected byte cacheByte;
    // The number of unread bits in cached byte.
    protected int leftBits = 0;

    private static final int DEFAULT_CAPACITY = 4096;

    protected BitBuffer() {
        this(DEFAULT_CAPACITY);
    }

    protected BitBuffer(int capacity) {
        this.buffer = ByteBuffer.allocate(capacity);
    }

    protected BitBuffer(ByteBuffer byteBuffer) {
        this.buffer = byteBuffer;
    }

    /**
     * Expand the capacity of buffer.
     */
    protected void expand() {
        int cap = buffer.capacity();
        ByteBuffer largerBuffer = ByteBuffer.allocateDirect((cap >> 1) + cap);
        buffer.flip();

        largerBuffer.put(buffer);
        //******************
//        largerBuffer.position(buffer.capacity());
        //******************
        buffer = largerBuffer;
    }

    /**
     * Returns the output buffer.
     */
    public ByteBuffer getBuffer() {
        return this.buffer;
    }

    /**
     * Flush the cache byte.
     */
    protected void flipByte() {

    }
}
