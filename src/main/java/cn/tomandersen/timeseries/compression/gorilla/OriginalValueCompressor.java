package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.BitWriter;
import fi.iki.yak.ts.compression.gorilla.Predictor;
import fi.iki.yak.ts.compression.gorilla.predictors.LastValuePredictor;

/**
 * <h3>OriginalValueCompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/12
 */
public class OriginalValueCompressor {

    private int storedLeadingZeros = Integer.MAX_VALUE;
    private int storedTrailingZeros = 0;

    // Output buffer for compressed metric value.
    protected final BitWriter output;

    // Predictor for metric value compression.
    protected final Predictor predictor;

    public OriginalValueCompressor(BitWriter output) {
        this.output = output;
        this.predictor = new LastValuePredictor();
    }

    public OriginalValueCompressor(BitWriter output, Predictor predictor) {
        this.output = output;
        this.predictor = predictor;
    }


    /**
     * Compress a value into specific buffer.
     *
     * @param value value to compress.
     */
    public void addValue(long value) {
        // In original Gorilla, Last-Value predictor is used
        long diff = predictor.predict() ^ value;
        predictor.update(value);

        if (diff == 0) {
            // Write '0' control bit.
            output.writeZeroBit();
        }
        else {
            int leadingZeros = Long.numberOfLeadingZeros(diff);
            int trailingZeros = Long.numberOfTrailingZeros(diff);

            output.writeOneBit(); // Optimize to writeNewLeading / writeExistingLeading?

            if (leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros) {
                writeExistingLeading(diff);
                //***********

//                storedLeadingZeros = leadingZeros;
//                storedTrailingZeros = trailingZeros;

                //***********
            }
            else {
                writeNewLeading(diff, leadingZeros, trailingZeros);
            }
        }
    }

    /**
     * Compress the first value in raw format, and update the storedLeadingZeros and storedTrailingZeros.
     */
    void writeFirst(long value) {
        output.writeBits(value, 64);
        predictor.update(value);

        //*************
//        storedLeadingZeros = Long.numberOfLeadingZeros(value);
//        storedTrailingZeros = Long.numberOfTrailingZeros(value);
        //*************

    }

    /**
     * If there at least as many leading zeros and as many trailing zeros as previous value, control bit = 0 (type a)
     * store the meaningful XORed value
     *
     * @param xor XOR between previous value and current
     */
    private void writeExistingLeading(long xor) {
        output.writeZeroBit();

        int significantBits = 64 - storedLeadingZeros - storedTrailingZeros;
        xor >>>= storedTrailingZeros;
        output.writeBits(xor, significantBits);
    }

    /**
     * store the length of the number of leading zeros in the next 5 bits
     * store length of the meaningful XORed value in the next 6 bits,
     * store the meaningful bits of the XORed value
     * (type b)
     *
     * @param xor           XOR between previous value and current
     * @param leadingZeros  New leading zeros
     * @param trailingZeros New trailing zeros
     */
    private void writeNewLeading(long xor, int leadingZeros, int trailingZeros) {
        output.writeOneBit();

        // Different from version 1.x, use (significantBits - 1) in storage - avoids a branch
        int significantBits = 64 - leadingZeros - trailingZeros;

        // Different from original, bits 5 -> 6, avoids a branch, allows storing small longs
        output.writeBits(leadingZeros, 6); // Number of leading zeros in the next 6 bits
        output.writeBits(significantBits - 1, 6); // Length of meaningful bits in the next 6 bits
        output.writeBits(xor >>> trailingZeros, significantBits); // Store the meaningful bits of XOR

        storedLeadingZeros = leadingZeros;
        storedTrailingZeros = trailingZeros;
    }

    /**
     * Close the buffer and stop compression.
     */
    public void close() {

    }

}
