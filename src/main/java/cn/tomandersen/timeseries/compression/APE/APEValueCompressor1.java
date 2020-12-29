package cn.tomandersen.timeseries.compression.APE;

import cn.tomandersen.timeseries.compression.APE.demos.APECompressionDemo;
import cn.tomandersen.timeseries.compression.MetricValueCompressor;
import fi.iki.yak.ts.compression.gorilla.BitOutput;
import fi.iki.yak.ts.compression.gorilla.Predictor;

/**
 * Difference with APEValueCompressor is about control bits between case A and case B.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 */
public class APEValueCompressor1 extends MetricValueCompressor {

    private int prevLeadingZeros = Integer.MAX_VALUE;
    private int prevTrailingZeros = Integer.MAX_VALUE;

    private long l0 = 0, l1 = 0;


    public APEValueCompressor1(BitOutput out) {
        super(out);
    }

    public APEValueCompressor1(BitOutput out, Predictor predictor) {
        super(out, predictor);
    }

    /**
     * Compress a value into specific buffer.
     *
     * @param value value to compress.
     */
    @Override
    public void addValue(long value) {
        // Calculate the XOR difference between prediction and current value to be compressed.
        long diff = predictor.predict() ^ value;
        predictor.update(value);
//        long diff = predict(value) ^ value;

        if (diff == 0) {
            // Write '0' bit as entire control bit(i.e. prediction and current value is same).
//            output.skipBit();
            output.writeBits(0b11, 2);

            APECompressionDemo.b0++;

//            APECompressionDemo.c0++;
//            APECompressionDemo.d0++;
        }
        else {
            int leadingZeros = Long.numberOfLeadingZeros(diff);
            int trailingZeros = Long.numberOfTrailingZeros(diff);

//            if (leadingZeros > 0 && leadingZeros < 16) APECompressionDemo.c0++;
//            else if (leadingZeros < 32) APECompressionDemo.c1++;
//            else APECompressionDemo.c2++;
//
//            if (trailingZeros > 0 && trailingZeros < 16) APECompressionDemo.d0++;
//            else if (trailingZeros < 32) APECompressionDemo.d1++;
//            else APECompressionDemo.d2++;

            /////
//            // Write '1' bit as first control bit.
//            output.writeBit();

            // If the scope of meaningful bits falls within the scope of previous meaningful bits,
            // i.e. there are at least as many leading zeros and as many trailing zeros as with
            // the previous value.
            if (leadingZeros >= prevLeadingZeros && trailingZeros >= prevTrailingZeros) {
                writeInPrevScope(diff);

                APECompressionDemo.b1++;
            }
            else {
                writeInNewScope(diff, leadingZeros, trailingZeros);

                APECompressionDemo.b2++;
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

        //******************
        // Write '10' as control bit
        output.writeBits(0b10, 2);
        //******************

//        // Write '0' bit as second control bit.
//        output.skipBit();

        // Write significant bits of difference value input the scope.
        int significantBits = 64 - prevLeadingZeros - prevTrailingZeros;
        xor >>>= prevTrailingZeros;
//        output.writeBits(xor, significantBits);
        // Since the first bit of significant bits must be '1', we can utilize it to store less bits.
        output.writeBits(xor, significantBits - 1);
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
        //******************
//        // Write '1' bit as second control bit.
//        output.writeBit();
//        // Write '0' bit as second control bit.
//        output.skipBit();
        output.writeBits(0b1, 1);
        //******************


        //******************
        // Statistic analysis
        int d0 = leadingZeros - prevLeadingZeros, d1 = trailingZeros - prevTrailingZeros;
        if (d0 >= -2 && d0 < 2) APECompressionDemo.c0++;
        else if (d0 >= -8 && d0 < 8) APECompressionDemo.c1++;
        else APECompressionDemo.c2++;

        d1 = d1 + d0;
        if (d1 >= -2 && d1 < 2) APECompressionDemo.d0++;
        else if (d1 >= -8 && d1 < 8) APECompressionDemo.d1++;
        else APECompressionDemo.d2++;
        //******************


        int significantBits = 64 - leadingZeros - trailingZeros;

        // Different from original, 5 -> 6 bits to store the number of leading zeros,
        // avoids the special situation when high precision xor value appears.
        output.writeBits(leadingZeros, 6); // Write the number of leading zeros input the next 6 bits
        // Since 'significantBits == 0' is unoccupied, we can just store 'significantBits - 1' to
        // cover a larger range and avoid the situation when 'significantBits == 64'.
        output.writeBits(significantBits - 1, 6); // Write the length of meaningful bits input the next 6 bits
        // Since the first bit of significant bits must be '1', we can utilize it to store less bits.
        output.writeBits(xor >>> trailingZeros, significantBits - 1); // Write the meaningful bits of XOR

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