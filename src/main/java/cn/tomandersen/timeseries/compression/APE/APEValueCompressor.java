package cn.tomandersen.timeseries.compression.APE;

import cn.tomandersen.timeseries.compression.APE.demos.APECompressionDemo;
import cn.tomandersen.timeseries.compression.MetricValueCompressor;
import fi.iki.yak.ts.compression.gorilla.BitOutput;
import fi.iki.yak.ts.compression.gorilla.Predictor;

/**
 * <h3>APE ValueCompressor</h3>
 * Difference from Gorilla Value Compressor is about the predict method.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 */
public class APEValueCompressor extends MetricValueCompressor {

    private int prevLeadingZeros = Integer.MAX_VALUE;
    private int prevTrailingZeros = Integer.MAX_VALUE;

    private long l0 = 0, l1 = 0;


    public APEValueCompressor(BitOutput out) {
        super(out);
    }

    public APEValueCompressor(BitOutput out, Predictor predictor) {
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
//        long diff = predictor.predict() ^ value;
//        predictor.update(value);
        long diff = predict(value) ^ value;

        if (diff == 0) {
            // Write '0' bit as entire control bit(i.e. prediction and current value is same).
            output.skipBit();

            APECompressionDemo.b0++;
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
                output.skipBit();
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
