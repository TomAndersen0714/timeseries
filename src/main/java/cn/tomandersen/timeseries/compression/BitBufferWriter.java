package cn.tomandersen.timeseries.compression;

import java.nio.ByteBuffer;

/**
 * <h3>BitBufferWriter</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/3
 */
public class BitBufferWriter extends BitBuffer implements BitWriter {

    public BitBufferWriter() {
        super();
        // Cache first empty byte, and reset the 'leftBits'
        this.cacheByte = buffer.get(buffer.position());
        this.leftBits = Byte.SIZE;
    }

    public BitBufferWriter(ByteBuffer byteBuffer) {
        super(byteBuffer);
    }


    /**
     * Write a '1' bit into the buffer.
     */
    @Override
    @WaitForTest
    public void writeOneBit() {
        cacheByte |= (1 << (leftBits - 1));
        leftBits--;
        flipByte();
    }

    /**
     * Write a '0' bit into the buffer.
     */
    @Override
    @WaitForTest
    public void writeZeroBit() {
        leftBits--;
        flipByte();
    }

    /**
     * Write the specific least significant bits of value into the buffer.
     *
     * @param value value need to write
     * @param bits  the number of bits need to write
     */
    @Override
    @WaitForTest
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
     * Write a single byte into the buffer.
     *
     * @param b the byte value need to write.
     */
    @Override
    @WaitForTest
    public void writeByte(byte b) {
        writeBits(Byte.toUnsignedLong(b), Byte.SIZE);
    }

    /**
     * Write a int type value into the buffer.
     *
     * @param value the int type value need to write.
     */
    @Override
    @WaitForTest
    public void writeInt(int value) {
        writeBits(value, Integer.SIZE);
    }

    /**
     * Write a long type value into the buffer.
     *
     * @param value the long type value need to write.
     */
    @Override
    @WaitForTest
    public void writeLong(long value) {
        writeBits(value, Long.SIZE);
    }

    /**
     * Write a float type value into the buffer.
     *
     * @param value the float type value need to write.
     */
    @Override
    @WaitForTest
    public void writeFloat(float value) {
        writeBits(Float.floatToRawIntBits(value), Float.SIZE);
    }

    /**
     * Write a double type value into the buffer.
     *
     * @param value the double type value need to write.
     */
    @Override
    @WaitForTest
    public void writeDouble(double value) {
        writeBits(Double.doubleToRawLongBits(value), Double.SIZE);
    }

    /**
     * Write the cached byte into the buffer.
     */
    @Override
    @WaitForTest
    public void flush() {
        leftBits = 0;
        flipByte();
    }

    /**
     * If cached byte is full, then write it into buffer and get next empty byte.
     */
    @WaitForTest
    protected void flipByte() {
        if (leftBits == 0) {
            buffer.put(cacheByte);
            if (!buffer.hasRemaining())
                expand();

            cacheByte = buffer.get(buffer.position());
            leftBits = Byte.SIZE;
        }
    }

}
