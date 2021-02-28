package cn.tomandersen.timeseries.compression.bucket;

import cn.tomandersen.timeseries.compression.benchmark.APECompressionDemo;
import cn.tomandersen.timeseries.compression.BitWriter;
import cn.tomandersen.timeseries.compression.MetricValueCompressor;
import cn.tomandersen.timeseries.compression.predictor.Predictor;

/**
 * Bucket of bucket algorithm for time-series value compression.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 * @see BucketValueDecompressor
 */
public class BucketValueCompressor extends MetricValueCompressor {

    private int prevLeadingZeros = Long.SIZE;
    private int prevTrailingZeros = Long.SIZE;

    private static final int MASK_OFFSET_4 = 0b10 << 4;
    private static final int MASK_OFFSET_6 = 0b11 << 6;


    public BucketValueCompressor(BitWriter out) {
        super(out);
    }

    public BucketValueCompressor(BitWriter out, Predictor predictor) {
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
        long xor = predictor.predict() ^ value;
        predictor.update(value);
//        long diff = predict(value) ^ value;

        if (xor == 0) {// case A:
            // Write '11' bit as entire control bit(i.e. prediction and current value is same).
            // According the the distribution of values(i.e. entropy code).
//            output.writeZeroBit();
            output.writeBits(0b11, 2);

            APECompressionDemo.b0++;

//            APECompressionDemo.c0++;
//            APECompressionDemo.d0++;
        }
        else {
            int leadingZeros = Long.numberOfLeadingZeros(xor);
            int trailingZeros = Long.numberOfTrailingZeros(xor);

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
                // case B:
                writeInPrevScope(xor);

                APECompressionDemo.b1++;
            }
            else {// case C:
                writeInNewScope(xor, leadingZeros, trailingZeros);

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
        /*// Since the first bit of significant bits must be '1', we can utilize it to store less bits.
        output.writeBits(xor, significantBits - 1);*/
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
        //******************
        // Write '1' bit as second control bit.
//        output.writeOneBit();
        // Write '0' bit as second control bit.
//        output.writeZeroBit();
//        output.writeBits(0b1, 1);
        output.writeBits(0b0, 1);
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
        // In this case xor value don't equal to zero, so 'significantBits' will not be '0'
        // which we can leverage to reduce 'significantBits' by 1 to cover scope [1,64]
        //******************
        int diffLeadingZeros = encodeZigZag32(leadingZeros - prevLeadingZeros);
        int diffSignificantBits = encodeZigZag32(leadingZeros + trailingZeros - prevLeadingZeros - prevTrailingZeros);
        int leastSignificantBits = Integer.SIZE - Integer.numberOfLeadingZeros(diffLeadingZeros);
        switch (leastSignificantBits) {
            case 0:
            case 1:
            case 2:// '0' as entire control bit meaning the number of least significant bits of 
                // encoded 'diffLeadingZeros' equals 2
                output.writeZeroBit();
                // write the least significant bits of encoded 'diffLeadingZeros'
                output.writeBits(diffLeadingZeros, 2);
                APECompressionDemo.c0++;
                break;
            case 3:
            case 4:// '10' as entire control bit meaning the number of least significant bits of 
                // encoded 'diffLeadingZeros' equals 4
                output.writeBits(0b10, 2);
                // write the least significant bits of encoded 'diffLeadingZeros'
                output.writeBits(diffLeadingZeros, 4);
                APECompressionDemo.c1++;
                break;
            default:// '11' as entire control bit meaning just write the number of leading zeros in 6 bits
                output.writeBits(0b11, 2);
                output.writeBits(leadingZeros, 6);
                APECompressionDemo.c2++;
                break;
        }
        leastSignificantBits = Integer.SIZE - Integer.numberOfLeadingZeros(diffSignificantBits);
        switch (leastSignificantBits) {
            case 0:
            case 1:
            case 2:// '0' as entire control bit meaning the number of least significant bits of 
                // encoded 'diffSignificantBits' equals 2
                output.writeZeroBit();
                output.writeBits(diffSignificantBits, 2);
                APECompressionDemo.d0++;
                break;
            case 3:
            case 4:// '10' as entire control bit meaning the number of least significant bits of 
                // encoded 'diffSignificantBits' equals 4
                output.writeBits(0b10, 2);
                output.writeBits(diffSignificantBits, 4);
                APECompressionDemo.d1++;
                break;
            default:// '11' as entire control bit meaning just write the number of significant bits in 6 bits
                output.writeBits(0b11, 2);
                // In this case xor value don't equal to zero, so 'significantBits' will not be '0'
                // which we can leverage to reduce 'significantBits' by 1 to cover scope [1,64]
                output.writeBits(significantBits - 1, 6);
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
     * Close the buffer and stop compression.
     * <p>
     * PS: Current implements doesn't need close method.
     */
    @Override
    public void close() {
        isClosed = true;
        // Do nothing.
        output.flush();
    }
}