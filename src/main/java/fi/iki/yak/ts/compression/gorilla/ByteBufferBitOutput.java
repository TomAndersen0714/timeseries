package fi.iki.yak.ts.compression.gorilla;

import java.nio.ByteBuffer;

/**
 * An implementation of BitOutput interface that uses off-heap storage.
 *
 * @author Michael Burman
 */
public class ByteBufferBitOutput implements BitOutput {
    public static final int DEFAULT_ALLOCATION = 4096;

    private ByteBuffer buffer;
    private byte cacheByte;
    private int leftBits = Byte.SIZE;

    /**
     * Creates a new ByteBufferBitOutput with a default allocated size of 4096 bytes.
     */
    public ByteBufferBitOutput() {
        this(DEFAULT_ALLOCATION);
    }

    /**
     * Give an initialSize different than DEFAULT_ALLOCATIONS. Recommended to use values which are dividable by 4096.
     *
     * @param initialSize New initialsize to use
     */
    public ByteBufferBitOutput(int initialSize) {
        buffer = ByteBuffer.allocateDirect(initialSize);
        cacheByte = buffer.get(buffer.position());
    }

    private void expand() {
        ByteBuffer largerBB = ByteBuffer.allocateDirect(buffer.capacity() * 2);
        buffer.flip();
        largerBB.put(buffer);
        largerBB.position(buffer.capacity());
        buffer = largerBB;
    }

    private void flipByte() {
        if (leftBits == 0) {
            buffer.put(cacheByte);
            if (!buffer.hasRemaining()) {
                expand();
            }
            cacheByte = buffer.get(buffer.position());
            leftBits = Byte.SIZE;
        }
    }

    @Override
    public void writeBit() {
        cacheByte |= (1 << (leftBits - 1));
        leftBits--;
        flipByte();
    }

    @Override
    public void skipBit() {
        leftBits--;
        flipByte();
    }

    /**
     * Writes the given long to the stream using bits amount of meaningful bits.
     *
     * @param value Value to be written to the stream
     * @param bits  How many bits are stored to the stream
     */
    public void writeBits(long value, int bits) {
        while (bits > 0) {
            int shift = bits - leftBits;
            if (shift >= 0) {
                cacheByte |= (byte) ((value >> shift) & ((1 << leftBits) - 1));
                bits -= leftBits;
                leftBits = 0;
            }
            else {
                shift = leftBits - bits;
                cacheByte |= (byte) (value << shift);
                leftBits -= bits;
                bits = 0;
            }
            flipByte();
        }
    }

    /**
     * Causes the currently handled byte to be written to the stream
     */
    @Override
    public void flush() {
        leftBits = 0;
        flipByte(); // Causes write to the ByteBuffer
    }

    /**
     * Returns the underlying DirectByteBuffer
     *
     * @return ByteBuffer of type DirectByteBuffer
     */
    public ByteBuffer getByteBuffer() {
        return this.buffer;
    }
}
