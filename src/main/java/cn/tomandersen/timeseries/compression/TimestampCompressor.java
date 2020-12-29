package cn.tomandersen.timeseries.compression;

/**
 * @see TimestampDecompressor
 */
public abstract class TimestampCompressor {
    // End sign for timestamp compression.
    public static final int END_SIGN = -1;

    // Output buffer for compressed timestamp value.
    protected final BitWriter output;
    // Closing sign.
    protected boolean isClosed;

    public TimestampCompressor(BitWriter output) {
        this.output = output;
    }

    /**
     * Compress a timestamp into specific {@link BitWriter buffer stream}.
     */
    public abstract void addTimestamp(long timestamp);

    /**
     * Close the buffer and write special value into buffer as end sign.
     */
    public abstract void close();

    public BitWriter getOutput() {
        return output;
    }

    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Encode a 32-bit signed value(i.e. Integer type value) to unsigned value.
     *
     * @param n A signed 32-bit integer.
     * @return An unsigned 32-bit integer, stored input a signed int because
     * Java has no explicit unsigned support.
     */
    protected static int encodeZigZag32(final int n) {
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
    protected static long encodeZigZag64(final long n) {
        // The right-shift must be arithmetic.
        return (n << 1) ^ (n >> 63);
    }
}
