package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.MetricValueDecompressor;
import fi.iki.yak.ts.compression.gorilla.BitInput;
import fi.iki.yak.ts.compression.gorilla.Predictor;

/**
 * <h3>GorillaValueDecompressor</h3>
 * Decompressor for the block which is compressed by the {@link GorillaValueCompressor}.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/30
 */
public class GorillaValueDecompressor extends MetricValueDecompressor {


    private int prevLeadingZeros = 0;
    private int prevTrailingZeros = 0;

    public GorillaValueDecompressor(BitInput input) {
        // Default predictor is set LastValuePredictor(i.e. prediction and previous value is same).
        super(input);
    }

    public GorillaValueDecompressor(BitInput input, Predictor predictor) {
        super(input, predictor);
    }

    @Override
    public long nextValue() {
        // Read next value's control bits.
        int controlBits = input.nextClearBit(2), significantBitLength;
        long currentValue = 0, xorValue;
        long prediction = predictor.predict();

        // Match the case corresponding to the control bits.
        switch (controlBits) {
            case 0b0:
                // '0' bit (i.e. prediction(previous) and current value is same)
                currentValue = prediction;
                predictor.update(currentValue);
                break;

            case 0b10:
                // '10' bits (i.e. the block of current value meaningful bits falls within
                // the scope of prediction(previous) meaningful bits)

                // Read the significant bits and restore the xor value.
                significantBitLength = Long.SIZE - prevLeadingZeros - prevTrailingZeros;
                xorValue = input.getLong(significantBitLength) << prevTrailingZeros;
                currentValue = prediction ^ xorValue;
                predictor.update(currentValue);

                // Update the number of leading and trailing zeros of xor residual.
                prevLeadingZeros = Long.numberOfLeadingZeros(xorValue);
                prevTrailingZeros = Long.numberOfTrailingZeros(xorValue);

                break;

            case 0b11:
                // '11' bits (i.e. the block of current value meaningful bits doesn't falls within
                // the scope of previous meaningful bits)
                // Update the number of leading and trailing zeros.
                prevLeadingZeros = (int) input.getLong(6);
                significantBitLength = (int) input.getLong(6);
                // Since we have decreased the length of significant bits by 1 for larger compression range
                // when we compress it, we restore it's value here.
                significantBitLength++;

                // Read the significant bits and restore the xor value.
                prevTrailingZeros = Long.SIZE - prevLeadingZeros - significantBitLength;
                xorValue = input.getLong(significantBitLength) << prevTrailingZeros;
                currentValue = prediction ^ xorValue;
                predictor.update(currentValue);
                break;

            default:
                // Do nothing
        }
        return currentValue;
    }
}
