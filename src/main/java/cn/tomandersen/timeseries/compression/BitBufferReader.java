package cn.tomandersen.timeseries.compression;

import org.omg.PortableInterceptor.NON_EXISTENT;

import java.nio.ByteBuffer;

/**
 * <h3>BitBufferReader</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/3
 */
public class BitBufferReader extends BitBuffer implements BitReader {

    public BitBufferReader() {
        super();
    }

    public BitBufferReader(ByteBuffer byteBuffer) {
        super(byteBuffer);
    }

    /**
     * Read the next bit and returns true if is '1' bit and false if not.
     *
     * @return true(i.e. ' 1 ' bit), false(i.e. '0' bit)
     */
    @Override
    public boolean nextBit() {
        boolean bit = ((cacheByte >> (leftBits - 1)) & 0b1) == 1;
        leftBits--;
        flipByte();
        return bit;
    }

    /**
     * Read bit continuously, until next '0' bit is found or the number of
     * read bits reach the value of 'maxBits'.
     *
     * @param maxBits How many bits at maximum until returning
     * @return Integer value of the read bits
     */
    @Override
    public int nextControlBits(int maxBits) {
        int controlBits = 0x00;

        for (int i = 0; i < maxBits; i++) {
            controlBits <<= 1;
            boolean bit = nextBit();

            if (bit) {
                controlBits |= 0x01;
            }
            else {
                break;
            }
        }
        return controlBits;
    }

    /**
     * Read next 8 bits and returns the byte value.
     *
     * @return byte type value
     */
    @Override
    public byte nextByte() {
        return 0;
    }

    /**
     * Read next n(n<=8) bits and return the corresponding byte type value.
     *
     * @param bits the number of bit need to read
     * @return byte type value
     */
    @Override
    public byte nextByte(int bits) {
        return 0;
    }

    /**
     * Read next 32 bits and return the corresponding integer type value.
     *
     * @return integer type value.
     */
    @Override
    public int nextInt() {
        return 0;
    }

    /**
     * Read next n(n<=32) bits and return the corresponding integer type value.
     *
     * @param bits the number of bit to read.
     * @return integer type value.
     */
    @Override
    public int nextInt(int bits) {
        return 0;
    }

    /**
     * Read next 64 bits and return the corresponding long type value.
     *
     * @return long type value .
     */
    @Override
    public long nextLong() {
        return 0;
    }

    /**
     * Read next n(n<=64) bits and return the corresponding long type value.
     *
     * @param bits the number of bit to read.
     * @return long type value.
     */
    @Override
    public long nextLong(int bits) {
        long value = 0;
        while (bits > 0) {
            if (bits > leftBits || bits == Byte.SIZE) {
                // Take only the least `leftBits` significant bits
                byte d = (byte) (cacheByte & ((1 << leftBits) - 1));
                value = (value << leftBits) + (d & 0xFF);
                bits -= leftBits;
                leftBits = 0;
            }
            else {
                // Shift to correct position and take only least significant bits
                byte d = (byte) ((cacheByte >>> (leftBits - bits)) & ((1 << bits) - 1));
                value = (value << bits) + (d & 0xFF);
                leftBits -= bits;
                bits = 0;
            }
            flipByte();
        }
        return value;
    }

    /**
     * Read next 32 bits and return the corresponding float type value.
     *
     * @return float type value.
     */
    @Override
    public float nextFloat() {
        return 0;
    }

    /**
     * Read next n(n<=32) bits and return the corresponding float type value.
     *
     * @param bits the number of bit to read
     * @return float type value.
     */
    @Override
    public float nextFloat(int bits) {
        return 0;
    }

    /**
     * Read next 64 bits and return the corresponding double type value.
     *
     * @return double type value.
     */
    @Override
    public double nextDouble() {
        return 0;
    }

    /**
     * Read next n(n<=64) bits and return the corresponding double type value.
     *
     * @param bits the number of bit to read.
     * @return double type value.
     */
    @Override
    public double nextDouble(int bits) {
        return 0;
    }

    // Get a new byte from buffer, if all bits in cached byte have been read.
    private void flipByte() {
        if (leftBits == 0) {
            cacheByte = buffer.get();
            leftBits = Byte.SIZE;
        }
    }
}
