package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.MetricValueCompressor;
import cn.tomandersen.timeseries.compression.gorilla.demos.GorillaCompressionDemo;
import fi.iki.yak.ts.compression.gorilla.BitOutput;
import fi.iki.yak.ts.compression.gorilla.Predictor;

/**
 * <h3>GorillaValueCompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/26
 */
public class GorillaValueCompressor extends MetricValueCompressor {

    //    private int prevLeadingZeros = Integer.MAX_VALUE;
    private int prevLeadingZeros = Integer.MAX_VALUE;
    private int prevTrailingZeros = Integer.MAX_VALUE;

    public GorillaValueCompressor(BitOutput out) {
        // Last-Value predictor is used input original Gorilla compression implement.
        super(out);
    }

    public GorillaValueCompressor(BitOutput out, Predictor predictor) {
        super(out, predictor);
    }

    /**
     * Compress a value into specific buffer.
     *
     * @param value value to compress.
     */
    public void addValue(long value) {
        // Calculate the XOR difference between prediction and current value to be compressed.
        long diff = predictor.predict() ^ value;
        predictor.update(value);

        if (diff == 0) {
            // Write '0' bit as entire control bit(i.e. prediction and current value is same).
            output.skipBit();
            GorillaCompressionDemo.b0++;
        }
        else {
            int leadingZeros = Long.numberOfLeadingZeros(diff);
            int trailingZeros = Long.numberOfTrailingZeros(diff);

            // Write '1' bit as first control bit.
            output.writeBit();

            // If the scope of meaningful bits falls within the scope of previous meaningful bits,
            // i.e. there are at least as many leading zeros and as many trailing zeros as with
            // the previous value.
            if (leadingZeros >= prevLeadingZeros && trailingZeros >= prevTrailingZeros) {
                writeInPrevScope(diff);
                GorillaCompressionDemo.b1++;
            }
            else {
                writeInNewScope(diff, leadingZeros, trailingZeros);
                GorillaCompressionDemo.b2++;

//                // Update the number of leading and trailing zeros.
//                prevLeadingZeros = leadingZeros;
//                prevTrailingZeros = trailingZeros;
            }
            // Update the number of leading and trailing zeros.
            prevLeadingZeros = leadingZeros;
            prevTrailingZeros = trailingZeros;
        }
    }

    /**
     * Case A:
     * If there at least as many leading zeros and as many trailing zeros as previous value.
     * <p>
     * Store the second control bit '0' representing the case a.
     *
     * @param xor XOR between previous value and current
     */
    private void writeInPrevScope(long xor) {
        // Write '0' bit as second control bit.
        output.skipBit();

        // Write significant bits of difference value input the scope.
        int significantBits = 64 - prevLeadingZeros - prevTrailingZeros;
        xor >>>= prevTrailingZeros;
        output.writeBits(xor, significantBits);
    }

    /**
     * Case B:
     * Store the second control bit '1' representing the case b,
     * store the length of leading zeros input the next 5 bits,
     * store the length of the meaningful XORed value input the next 6 bits,
     * store the meaningful bits of the XORed value.
     *
     * @param xor           XOR between previous delta value and current
     * @param leadingZeros  New number of leading zeros
     * @param trailingZeros New number of trailing zeros
     */
    private void writeInNewScope(long xor, int leadingZeros, int trailingZeros) {
        // Write '1' bit as second control bit.
        output.writeBit();

        //******************
        // Statistic analysis

        int d0 = leadingZeros - prevLeadingZeros, d1 = trailingZeros - prevTrailingZeros;
        if (d0 >= -4 && d0 < 4) GorillaCompressionDemo.c0++;
        else if (d0 >= -8 && d0 < 8) GorillaCompressionDemo.c1++;
        else GorillaCompressionDemo.c2++;

        d1 = d1 + d0;
        if (d1 >= -4 && d1 < 4) GorillaCompressionDemo.d0++;
        else if (d1 >= -8 && d1 < 8) GorillaCompressionDemo.d1++;
        else GorillaCompressionDemo.d2++;
        //******************

        int significantBits = 64 - leadingZeros - trailingZeros;

        // Different from original, 5 -> 6 bits to store the number of leading zeros,
        // avoids the special situation when high precision xor value appears.
        output.writeBits(leadingZeros, 6); // Write the number of leading zeros input the next 6 bits
        // Since 'significantBits == 0' is unoccupied, we can just store 'significantBits - 1' to
        // cover a larger range and avoid the situation when 'significantBits == 64'.
        output.writeBits(significantBits - 1, 6); // Write the length of meaningful bits input the next 6 bits
        output.writeBits(xor >>> trailingZeros, significantBits); // Write the meaningful bits of XOR

//        // Update the number of leading and trailing zeros.
//        prevLeadingZeros = leadingZeros;
//        prevTrailingZeros = trailingZeros;
    }

    /**
     * Close the buffer and stop compression.
     * <p>
     * PS: Current implements doesn't need close method.
     */
    @Override
    public void close() {
        isClosed = true;
        // Do nothing.
    }
}
