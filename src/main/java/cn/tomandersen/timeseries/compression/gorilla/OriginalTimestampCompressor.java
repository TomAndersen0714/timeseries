package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.BitWriter;

/**
 * <h3>OriginalTimestampCompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/12
 */
public class OriginalTimestampCompressor {

    private long blockTimestamp;
    private int storedDelta;
    private long storedTimestamp;

    private BitWriter output;

    private static final int OFFSET_7_MASK = 0b10 << 7;
    private static final int OFFSET_9_MASK = 0b110 << 9;
    private static final int OFFSET_12_MASK = 0b1110 << 12;
    private static final int FIRST_DELTA_BITS = 27;

    public OriginalTimestampCompressor(BitWriter output) {
        this.output = output;

    }

    /**
     * Add a timestamp in raw format as the header of current block.
     */
    public void addHeader(long timestamp) {
        blockTimestamp = timestamp;
        output.writeBits(timestamp, 64);
    }


    /**
     * Compress a timestamp into specific {@link BitWriter} buffer.
     */
    public void addTimestamp(long timestamp) {

        // a) Calculate the delta of delta
        int newDelta = (int) (timestamp - storedTimestamp);
        int deltaD = newDelta - storedDelta;

        if (deltaD == 0) {
            output.writeZeroBit();
        }
        else {
            deltaD = encodeZigZag32(deltaD);
            deltaD--; // Increase by one in the decompressing phase as we have one free bit
            int bitsRequired = 32 - Integer.numberOfLeadingZeros(deltaD); // Faster than highestSetBit

            switch (bitsRequired) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    deltaD |= OFFSET_7_MASK;
                    output.writeBits(deltaD, 9);
                    break;
                case 8:
                case 9:
                    deltaD |= OFFSET_9_MASK;
                    output.writeBits(deltaD, 12);
                    break;
                case 10:
                case 11:
                case 12:
                    output.writeBits(deltaD | OFFSET_12_MASK, 16);
                    break;
                default:
                    output.writeBits(0x0F, 4); // Store '1111'
                    output.writeBits(deltaD, 32); // Store delta using 32 bits
                    break;
            }
            storedDelta = newDelta;
        }
        storedTimestamp = timestamp;
    }

    /**
     * Compress the first value in raw format.
     */
    void writeFirst(long timestamp) {
        storedDelta = (int) (timestamp - blockTimestamp);
        storedTimestamp = timestamp;
        output.writeBits(timestamp, FIRST_DELTA_BITS);
    }

    /**
     * Close the buffer and write special value into buffer as end sign.
     */
    public void close() {
        output.writeBits(0x0F, 4);
        output.writeBits(0xFFFFFFFF, 32);
        output.writeZeroBit();
        output.flush();
    }

    /**
     * Encode a 32-bit signed value(i.e. Integer type value) to unsigned value.
     *
     * @param n A signed 32-bit integer.
     * @return An unsigned 32-bit integer, stored input a signed int because
     * Java has no explicit unsigned support.
     */
    private static int encodeZigZag32(final int n) {
        // Note: the right-shift must be arithmetic
        return (n << 1) ^ (n >> 31);
    }

    /**
     * Encode a 64-bit signed value(i.e. Integer type value) to unsigned value.
     *
     * @param n A signed 64-bit integer.
     * @return An unsigned 64-bit integer, stored input a signed int because
     * Java has no explicit unsigned type support.
     */
    private static long encodeZigZag64(final long n) {
        // The right-shift must be arithmetic.
        return (n << 1) ^ (n >> 63);
    }
}
