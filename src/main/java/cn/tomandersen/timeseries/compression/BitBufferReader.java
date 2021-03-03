package cn.tomandersen.timeseries.compression;

import java.nio.ByteBuffer;

/**
 * <h3>BitBufferReader</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/3
 */
public class BitBufferReader extends BitBuffer implements BitReader {

    public BitBufferReader(ByteBuffer inputByteBuffer) {
        super(inputByteBuffer);
        // Cache first byte.
        flipByte();
    }

    /**
     * Read the next bit and returns true if is '1' bit and false if not.
     *
     * @return true(i.e. ' 1 ' bit), false(i.e. '0' bit)
     */
    @Override
    @WaitForTest
    public boolean nextBit() {
        boolean bit = ((cacheByte >> (leftBits - 1)) & 1) == 1;
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
    @WaitForTest
    public int nextControlBits(int maxBits) {
        int controlBits = 0x00;

        for (int i = 0; i < maxBits; i++) {
            controlBits <<= 1;
            boolean bit = nextBit();

            if (bit) {
                // add a '1' bit to the end
                controlBits |= 0x01;
            }
            else {
                // add a '0' bit to the end
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
    @WaitForTest
    public byte nextByte() {
        return nextByte(Byte.SIZE);
    }

    /**
     * Read next n(n<=8) bits and return the corresponding byte type value.
     *
     * @param bits the number of bit need to read
     * @return byte type value
     */
    @Override
    @WaitForTest
    public byte nextByte(int bits) {
        byte value = 0;
        while (bits > 0 && bits <= Byte.SIZE) {
            if (bits > leftBits) {
                // Take only the leftBits "least significant" bits
                byte leastSignificantBits = (byte) (cacheByte & ((1 << leftBits) - 1));
                value = (byte) ((value << leftBits) + (leastSignificantBits & 0xFF));
                bits -= leftBits;
                leftBits = 0;
            }
            else {
                // Shift to correct position and take only least significant bits
                byte leastSignificantBits = (byte) ((cacheByte >>> (leftBits - bits)) & ((1 << bits) - 1));
//                value = (byte) ((value << bits) + (leastSignificantBits & 0xFF));
                value = (byte) ((value << bits) & leastSignificantBits);
                leftBits -= bits;
                bits = 0;
            }
            flipByte();
        }
        return value;
    }

    /**
     * Read next 32 bits and return the corresponding integer type value.
     *
     * @return integer type value.
     */
    @Override
    @WaitForTest
    public int nextInt() {
        return nextInt(Integer.SIZE);
    }

    /**
     * Read next n(n<=32) bits and return the corresponding integer type value.
     *
     * @param bits the number of bit to read.
     * @return integer type value.
     */
    @Override
    @WaitForTest
    public int nextInt(int bits) {
        int value = 0;
        while (bits > 0 && bits <= Integer.SIZE) {
            if (bits > leftBits || bits == Byte.SIZE) {
                // Take only the least `leftBits` significant bits
                byte leastSignificantBits = (byte) (cacheByte & ((1 << leftBits) - 1));
                value = (value << leftBits) + (leastSignificantBits & 0xFF);
                bits -= leftBits;
                leftBits = 0;
            }
            else {
                // Shift to correct position and take only least significant bits
                byte leastSignificantBits = (byte) ((cacheByte >>> (leftBits - bits)) & ((1 << bits) - 1));
                value = (value << bits) | leastSignificantBits;
                leftBits -= bits;
                bits = 0;
            }
            flipByte();
        }
        return value;
    }

    /**
     * Read next 64 bits and return the corresponding long type value.
     *
     * @return long type value .
     */
    @Override
    @WaitForTest
    public long nextLong() {
        return nextLong(Long.SIZE);
    }

    /**
     * Read next n(n<=64) bits and return the corresponding long type value.
     *
     * @param bits the number of bit to read.
     * @return long type value.
     */
    @Override
    @WaitForTest
    public long nextLong(int bits) {
        long value = 0;
        while (bits > 0 && bits <= Long.SIZE) {
            if (bits > leftBits || bits == Byte.SIZE) {
                // Take only the least `leftBits` significant bits
                byte leastSignificantBits = (byte) (cacheByte & ((1 << leftBits) - 1));
                value = (value << leftBits) + (leastSignificantBits & 0xFF);
                bits -= leftBits;
                leftBits = 0;
            }
            else {
                // Shift to correct position and take only least significant bits
                byte leastSignificantBits = (byte) ((cacheByte >>> (leftBits - bits)) & ((1 << bits) - 1));
                value = (value << bits) + (leastSignificantBits & 0xFF);
//                value = (value << bits) | leastSignificantBits;
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
    @WaitForTest
    public float nextFloat() {
        return nextFloat(Float.SIZE);
    }

    /**
     * Read next n(n<=32) bits and return the corresponding float type value.
     *
     * @param bits the number of bit to read
     * @return float type value.
     */
    @Override
    @WaitForTest
    public float nextFloat(int bits) {
        int value = 0;
        while (bits > 0 && bits <= Integer.SIZE) {
            if (bits > leftBits || bits == Byte.SIZE) {
                // Take only the least `leftBits` significant bits
                byte leastSignificantBits = (byte) (cacheByte & ((1 << leftBits) - 1));
                value = (value << leftBits) + (leastSignificantBits & 0xFF);
                bits -= leftBits;
                leftBits = 0;
            }
            else {
                // Shift to correct position and take only least significant bits
                byte leastSignificantBits = (byte) ((cacheByte >>> (leftBits - bits)) & ((1 << bits) - 1));
                value = (value << bits) | leastSignificantBits;
//                value = (value << bits) & leastSignificantBits;
                leftBits -= bits;
                bits = 0;
            }
            flipByte();
        }
        return Float.intBitsToFloat(value);
    }

    /**
     * Read next 64 bits and return the corresponding double type value.
     *
     * @return double type value.
     */
    @Override
    @WaitForTest
    public double nextDouble() {
        return nextDouble(64);
    }

    /**
     * Read next n(n<=64) bits and return the corresponding double type value.
     *
     * @param bits the number of bit to read.
     * @return double type value.
     */
    @Override
    @WaitForTest
    public double nextDouble(int bits) {
        long value = 0;
        while (bits > 0 && bits <= Long.SIZE) {
            if (bits > leftBits || bits == Byte.SIZE) {
                // Take only the least `leftBits` significant bits
                byte leastSignificantBits = (byte) (cacheByte & ((1 << leftBits) - 1));
                value = (value << leftBits) + (leastSignificantBits & 0xFF);
                bits -= leftBits;
                leftBits = 0;
            }
            else {
                // Shift to correct position and take only least significant bits
                byte leastSignificantBits = (byte) ((cacheByte >>> (leftBits - bits)) & ((1 << bits) - 1));
                value = (value << bits) | leastSignificantBits;
//                value = (value << bits) & leastSignificantBits;
                leftBits -= bits;
                bits = 0;
            }
            flipByte();
        }
        return Double.longBitsToDouble(value);
    }

    /**
     * Get a new byte from buffer, if all bits in cached byte have been read.
     */
    @WaitForTest
    protected void flipByte() {
        if (leftBits == 0) {
            cacheByte = buffer.get();
            leftBits = Byte.SIZE;
        }
    }
}
