package cn.tomandersen.timeseries.compression;

import fi.iki.yak.ts.compression.gorilla.Predictor;
import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;

public abstract class MetricValueCompressor {

    // Output buffer for compressed metric value.
    protected final BitWriter output;
    protected boolean isClosed;

    // Predictor for metric value compression.
    protected Predictor predictor;

    protected MetricValueCompressor(BitWriter output) {
        // Default predictor is LastValuePredictor.
        this(output, new LastValuePredictor());
    }

    protected MetricValueCompressor(BitWriter output, Predictor predictor) {
        this.output = output;
        this.predictor = predictor; // Custom predictor.
    }

    /**
     * Compress a value into specific buffer.
     *
     * @param value value to compress.
     */
    public abstract void addValue(long value);

    public void addValue(double value) {
        addValue(Double.doubleToRawLongBits(value));
    }

    /**
     * Close the buffer and stop compression.
     */
    public abstract void close();


    public BitWriter getOutput() {
        return output;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public Predictor getPredictor() {
        return predictor;
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
