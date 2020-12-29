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
    }

    public BitBufferWriter(ByteBuffer byteBuffer) {
        super(byteBuffer);
    }


    /**
     * Write a '1' bit into the buffer.
     */
    @Override
    public void writeOneBit() {
        cacheByte |= (1 << (leftBits - 1));
        leftBits--;
        flipByte();
    }

    /**
     * Write a '0' bit into the buffer.
     */
    @Override
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
    public void writeByte(byte b) {
        writeBits(Byte.toUnsignedLong(b), 8);
    }

    /**
     * Write a int type value into the buffer.
     *
     * @param value the int type value need to write.
     */
    @Override
    public void writeInt(int value) {
        writeBits(value, 32);
    }

    /**
     * Write a long type value into the buffer.
     *
     * @param value the long type value need to write.
     */
    @Override
    public void writeLong(long value) {
        writeBits(value, 64);
    }

    /**
     * Write a float type value into the buffer.
     *
     * @param value the float type value need to write.
     */
    @Override
    public void writeFloat(float value) {
        writeBits(Float.floatToRawIntBits(value), 32);
    }

    /**
     * Write a double type value into the buffer.
     *
     * @param value the double type value need to write.
     */
    @Override
    public void writeDouble(double value) {
        writeBits(Double.doubleToRawLongBits(value), 64);
    }

    /**
     * Write the cached byte into the buffer.
     */
    @Override
    public void flush() {
        leftBits = 0;
        flipByte();
    }

    /**
     * Expand the capacity of buffer.
     */
    private void expand() {
        ByteBuffer largerBuffer = ByteBuffer.allocateDirect(buffer.capacity() * 2);
        buffer.flip();

        largerBuffer.put(buffer);
        //******************
//        largerBuffer.position(buffer.capacity());
        //******************
        buffer = largerBuffer;
    }

    /**
     * If cached byte is full, then write it into buffer and get next empty byte.
     */
    private void flipByte() {
        if (leftBits == 0) {
            if (!buffer.hasRemaining())
                expand();
            buffer.put(cacheByte);

            cacheByte = buffer.get(buffer.position());
            leftBits = Byte.SIZE;
        }
    }

}
