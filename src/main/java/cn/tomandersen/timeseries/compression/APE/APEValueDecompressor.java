package cn.tomandersen.timeseries.compression.APE;

import cn.tomandersen.timeseries.compression.BitReader;
import cn.tomandersen.timeseries.compression.MetricValueDecompressor;
import fi.iki.yak.ts.compression.gorilla.Predictor;

/**
 * <h3>APE ValueDecompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 */
public class APEValueDecompressor extends MetricValueDecompressor {

    private int prevLeadingZeros = 0;
    private int prevTrailingZeros = 0;

    private long l0 = 0, l1 = 0;

    public APEValueDecompressor(BitReader input) {
        super(input);
    }

    public APEValueDecompressor(BitReader input, Predictor predictor) {
        super(input, predictor);
    }


    /**
     * Decompress a long value from the specific buffer stream.
     *
     * @return decompressed value input long type.
     */
    @Override
    public long nextValue() {
        // Get prediction first.
        long prediction = predict();
        // Read next value's control bits.
        int controlBits = input.nextControlBits(2), significantBitLength;
        long currentValue = 0, xorValue;


        // Match the case corresponding to the control bits.
        switch (controlBits) {
            case 0b0:
                // '0' bit (i.e. prediction(previous) and current value is same)
                currentValue = prediction;
//                predictor.update(currentValue);
                break;

            case 0b10:
                // '10' bits (i.e. the block of current value meaningful bits falls within
                // the scope of prediction(previous) meaningful bits)

                // Read the significant bits and restore the xor value.
                significantBitLength = Long.SIZE - prevLeadingZeros - prevTrailingZeros;
                xorValue = input.nextLong(significantBitLength) << prevTrailingZeros;
                currentValue = prediction ^ xorValue;
//                predictor.update(currentValue);

                // Update the number of leading and trailing zeros of xor residual.
                prevLeadingZeros = Long.numberOfLeadingZeros(xorValue);
                prevTrailingZeros = Long.numberOfTrailingZeros(xorValue);

                break;

            case 0b11:
                // '11' bits (i.e. the block of current value meaningful bits doesn't falls within
                // the scope of previous meaningful bits)
                // Update the number of leading and trailing zeros.
                prevLeadingZeros = (int) input.nextLong(6);
                significantBitLength = (int) input.nextLong(6);
                // Since we have decreased the length of significant bits by 1 for larger compression range
                // when we compress it, we restore it's value here.
                significantBitLength++;

                // Read the significant bits and restore the xor value.
                prevTrailingZeros = Long.SIZE - prevLeadingZeros - significantBitLength;
                xorValue = input.nextLong(significantBitLength) << prevTrailingZeros;
                currentValue = prediction ^ xorValue;
//                predictor.update(currentValue);
                break;

            default:
                // Do nothing
        }
        // Update cached previous value.
        l0 = l1;
        l1 = currentValue;
        return currentValue;
    }


    public long predict() {
        // Calculate the predicted value in different model.
        long x0 = l0, x1 = l1, x2 = l1 + l1 - l0;
//        // Update cached previous value.
//        l0 = l1;
//        l1 = value;
        // Read next selection info about prediction model.
        int controlBits = input.nextControlBits(2);
        switch (controlBits) {
            case 0b0:
                return x1;
            case 0b10:
                return x0;
            case 0b11:
                return x2;
        }
        return 0;
    }
}
