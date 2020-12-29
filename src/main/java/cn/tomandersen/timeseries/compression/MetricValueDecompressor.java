package cn.tomandersen.timeseries.compression;

import fi.iki.yak.ts.compression.gorilla.BitInput;
import fi.iki.yak.ts.compression.gorilla.Predictor;
import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;

/**
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/27
 */
public abstract class MetricValueDecompressor {

    protected final BitInput input;
    protected final Predictor predictor;


    protected MetricValueDecompressor(BitInput input) {
        // Default predictor is LastValuePredictor.
        this(input, new LastValuePredictor());
    }

    protected MetricValueDecompressor(BitInput input, Predictor predictor) {
        this.input = input;
        this.predictor = predictor;
    }

    /**
     * Decompress a long value from the specific buffer stream.
     *
     * @return decompressed value input long type.
     */
    public abstract long nextValue();

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
