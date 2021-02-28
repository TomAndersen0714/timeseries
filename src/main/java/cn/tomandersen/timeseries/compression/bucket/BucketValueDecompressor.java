package cn.tomandersen.timeseries.compression.bucket;

import cn.tomandersen.timeseries.compression.BitReader;
import cn.tomandersen.timeseries.compression.MetricValueDecompressor;
import cn.tomandersen.timeseries.compression.predictor.Predictor;

/**
 * BucketValueDecompressor
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2021/1/9
 */
public class BucketValueDecompressor extends MetricValueDecompressor {
    private int prevLeadingZeros = 0;
    private int prevTrailingZeros = 0;

    public BucketValueDecompressor(BitReader input) {
        super(input);
    }

    protected BucketValueDecompressor(BitReader input, Predictor predictor) {
        super(input, predictor);
    }

    /**
     * Decompress a long value from the specific buffer stream.
     *
     * @return decompressed value input long type.
     */
    @Override
    public long nextValue() {
        // Read next value's control bits.
        int leadingZeros = 0, trailingZeros = 0, significantBits = 0;
        long currentValue = 0, xorValue;
        long prediction = predictor.predict();

        // Match the case corresponding to the control bits.
        int controlBits = input.nextControlBits(2);
        switch (controlBits) {
            case 0b0: // '0' as entire control bit(i.e. next value is in a new scope).

                // Get the number of leading zeros and significant bits of next value.
                int diffLeadingZeros, diffSignificantBits;
                controlBits = input.nextControlBits(2);
                switch (controlBits) {
                    case 0b0:// '0' as entire control bit meaning the number of least significant bits of
                        // encoded 'diffLeadingZeros' equals 2
                        diffLeadingZeros = input.nextInt(2);
                        diffLeadingZeros = decodeZigZag32(diffLeadingZeros);
                        leadingZeros = diffLeadingZeros + prevLeadingZeros;
                        break;
                    case 0b10:// '10' as entire control bit meaning the number of least significant bits of
                        // encoded 'diffLeadingZeros' equals 4
                        diffLeadingZeros = input.nextInt(4);
                        diffLeadingZeros = decodeZigZag32(diffLeadingZeros);
                        leadingZeros = diffLeadingZeros + prevLeadingZeros;
                        break;
                    case 0b11:// '11' as entire control bit meaning just write the number of leading zeros
                        // in 6 bits
                        leadingZeros = input.nextInt(6);
                        break;
                    default:
                        // Do nothing
                        break;
                }

                controlBits = input.nextControlBits(2);
                switch (controlBits) {
                    case 0b0:// '0' as entire control bit meaning the number of least significant bits of
                        // encoded 'diffSignificantBits' equals 2
                        diffSignificantBits = input.nextInt(2);
                        diffSignificantBits = decodeZigZag32(diffSignificantBits);
                        trailingZeros = diffSignificantBits +
                                prevLeadingZeros + prevTrailingZeros - leadingZeros;
                        significantBits = Long.SIZE - leadingZeros - trailingZeros;
                        break;
                    case 0b10:// '10' as entire control bit meaning the number of least significant bits of
                        // encoded 'diffSignificantBits' equals 4
                        diffSignificantBits = input.nextInt(4);
                        diffSignificantBits = decodeZigZag32(diffSignificantBits);
                        trailingZeros = diffSignificantBits +
                                prevLeadingZeros + prevTrailingZeros - leadingZeros;
                        significantBits = Long.SIZE - leadingZeros - trailingZeros;
                        break;
                    case 0b11:// '11' as entire control bit meaning just write the number of significant bits
                        // in 6 bits
                        significantBits = input.nextInt(6);
                        trailingZeros = Long.SIZE - leadingZeros - significantBits;
                        break;
                    default:
                        // Do nothing
                        break;
                }

                // Read the next xor value according to the 'trailingZeros' and 'significantBits'
                // Since we reduce the 'significantBitLength' by 1 when we write it, we need
                // to restore it here.
                xorValue = (input.nextLong(significantBits - 1) | (1 << (significantBits - 1)))
                        << trailingZeros;
                currentValue = prediction ^ xorValue;
                predictor.update(currentValue);

                // Update the number of leading and trailing zeros of current xor residual.
                prevLeadingZeros = leadingZeros;
                prevTrailingZeros = trailingZeros;
                break;

            case 0b10: // '10' bits (i.e. the block of next value meaningful bits falls within
                // the scope of prediction(previous value) meaningful bits)

                // Read the significant bits and restore the xor value.
                significantBits = Long.SIZE - prevLeadingZeros - prevTrailingZeros;
                xorValue = input.nextLong(significantBits) << prevTrailingZeros;
                currentValue = prediction ^ xorValue;
                predictor.update(currentValue);

                // Update the number of leading and trailing zeros of xor residual.
                prevLeadingZeros = Long.numberOfLeadingZeros(xorValue);
                prevTrailingZeros = Long.numberOfTrailingZeros(xorValue);
                break;

            case 0b11:// '11' as entire control bit(i.e. prediction and next value is same).
                currentValue = prediction;
                predictor.update(currentValue);
                break;

            default:
                // Do nothing

        }
        return currentValue;
    }
}
