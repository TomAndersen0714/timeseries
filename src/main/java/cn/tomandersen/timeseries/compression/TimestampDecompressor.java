package cn.tomandersen.timeseries.compression;

import java.nio.ByteBuffer;

/**
 * @see TimestampCompressor
 */
public abstract class TimestampDecompressor {

    // End sign for timestamp decompression.
    protected static final long END_SIGN = Long.MIN_VALUE;

    // Input buffer for compressed timestamp value.
    protected final BitReader input;
//    // Output buffer for decompressed timestamp value.
//    protected ByteBuffer output;

    protected TimestampDecompressor(BitReader input) {
        this.input = input;
    }

    /**
     * Decompress a timestamp from the specific buffer into long type.
     */
    public abstract long nextTimestamp();

    /**
     * Decode a ZigZag-encoded 32-bit value.
     *
     * @param n An unsigned 32-bit integer, stored input a signed int because Java has no explicit
     *          unsigned support.
     * @return A signed 32-bit integer.
     */
    protected static int decodeZigZag32(final int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Decode a ZigZag-encoded 64-bit value.
     *
     * @param n An unsigned 64-bit integer, stored input a signed int because Java has no explicit
     *          unsigned support.
     * @return A signed 64-bit integer.
     */
    protected static long decodeZigZag64(final long n) {
        return (n >>> 1) ^ -(n & 1);
    }
}
