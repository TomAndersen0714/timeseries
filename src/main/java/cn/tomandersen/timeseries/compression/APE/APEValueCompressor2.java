package cn.tomandersen.timeseries.compression.APE;

import cn.tomandersen.timeseries.compression.APE.demos.APECompressionDemo;
import cn.tomandersen.timeseries.compression.BitWriter;
import cn.tomandersen.timeseries.compression.MetricValueCompressor;
import cn.tomandersen.timeseries.compression.predictor.Predictor;

/**
 * Difference with APEValueCompressor is about control bits between case A and case B.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 */
public class APEValueCompressor2 extends MetricValueCompressor {

    private int prevLeadingZeros = Integer.MAX_VALUE;
    private int prevTrailingZeros = Integer.MAX_VALUE;

    private static final int MASK_OFFSET_4 = 0b10 << 4;
    private static final int MASK_OFFSET_6 = 0b11 << 6;

    private long l0 = 0, l1 = 0;


    public APEValueCompressor2(BitWriter out) {
        super(out);
    }

    public APEValueCompressor2(BitWriter out, Predictor predictor) {
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
//            output.writeZeroBit();
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
//            output.writeOneBit();

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

        //
        // Write '10' as control bit
        output.writeBits(0b10, 2);
        //

//        // Write '0' bit as second control bit.
//        output.writeZeroBit();

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
        // Write '1' bit as second control bit.
//        output.writeOneBit();
        // Write '0' bit as second control bit.
//        output.writeZeroBit();
        output.writeBits(0b1, 1);
        //******************

        //******************
        // Statistic analysis
//        int d0 = leadingZeros - prevLeadingZeros, d1 = trailingZeros - prevTrailingZeros;
//        if (d0 >= -2 && d0 < 2)
//            APECompressionDemo.c0++;
//        else if (d0 >= -8 && d0 < 8)
//            APECompressionDemo.c1++;
//        else
//            APECompressionDemo.c2++;
//
//        d1 = d1 + d0;
//        if (d1 >= -2 && d1 < 2)
//            APECompressionDemo.d0++;
//        else if (d1 >= -8 && d1 < 8)
//            APECompressionDemo.d1++;
//        else
//            APECompressionDemo.d2++;
        //******************


        int significantBits = 64 - leadingZeros - trailingZeros;
        //******************
        int diffLeadingZeros = encodeZigZag32(leadingZeros - prevLeadingZeros);
        int diffSignificantBits = encodeZigZag32(leadingZeros + trailingZeros - prevLeadingZeros - prevTrailingZeros);
        int leastSignificantBits = Integer.SIZE - Integer.numberOfLeadingZeros(diffLeadingZeros);
        switch (leastSignificantBits) {
            case 0:
            case 1:
            case 2:
                output.writeZeroBit();
                output.writeBits(diffLeadingZeros, 2);
                APECompressionDemo.c0++;
                break;
            case 3:
            case 4:
                output.writeBits(0b10, 2);
                output.writeBits(diffLeadingZeros, 4);
                APECompressionDemo.c1++;
                break;
            default:
                output.writeBits(0b11, 2);
                output.writeBits(leadingZeros, 6);
                APECompressionDemo.c2++;
                break;
        }
        leastSignificantBits = Integer.SIZE - Integer.numberOfLeadingZeros(diffSignificantBits);
        switch (leastSignificantBits) {
            case 0:
            case 1:
            case 2:
                output.writeZeroBit();
                output.writeBits(diffSignificantBits, 2);
                APECompressionDemo.d0++;
                break;
            case 3:
            case 4:
                output.writeBits(0b10, 2);
                output.writeBits(diffSignificantBits, 4);
                APECompressionDemo.d1++;
                break;
            default:
                output.writeBits(0b11, 2);
                output.writeBits(significantBits, 6);
                APECompressionDemo.d2++;
                break;
        }

        //******************
//        // Difference from original gorilla method, 5 -> 6 bits to store the number of leading zeros,
//        // avoids the special situation when high precision xor value appears.
//        output.writeBits(leadingZeros, 6); // Write the number of leading zeros input the next 6 bits
//
//        // Since 'significantBits == 0' is unoccupied, we can just store 'significantBits - 1' to
//        // cover a larger range and avoid the situation when 'significantBits == 64'.
//        output.writeBits(significantBits - 1, 6); // Write the length of meaningful bits input the next 6 bits

        // Since the first bit of significant bits must be '1', we can utilize it to store less bits.
        output.writeBits(xor >>> trailingZeros, significantBits - 1); // Write the meaningful bits of XOR

//        // Update the number of leading and trailing zeros.
//        prevLeadingZeros = leadingZeros;
//        prevTrailingZeros = trailingZeros;
    }

    /**
     * Select prediction mode and return predicted value.
     *
     * @param value value to compress.
     * @return predicted value
     */
    private long predict(long value) {
        // Calculate the predicted value in different model.
        long x0 = l0, x1 = l1, x2 = l1 + l1 - l0;
        long e0 = Math.abs(x0 - value), e1 = Math.abs(x1 - value), e2 = Math.abs(x2 - value);
        // Update cached previous value.
        l0 = l1;
        l1 = value;
        // Select best predicted value, write control bits which represent the selection info, and
        // return the best predicted value.
        if (e1 <= e0) {
            if (e1 <= e2) {
                // e1: Write the '0' control bit.
                output.writeZeroBit();
                return x1;
            }
            else {
                // e2: Write the '11' control bit.
                output.writeBits(0b11, 2);
                return x2;
            }
        }
        else {
            if (e0 <= e2) {
                // e0: Write the '10' control bit.
                output.writeBits(0b10, 2);
                return x0;
            }
            else {
                // e2: Write the '11' control bit.
                output.writeBits(0b11, 2);
                return x2;
            }
        }
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