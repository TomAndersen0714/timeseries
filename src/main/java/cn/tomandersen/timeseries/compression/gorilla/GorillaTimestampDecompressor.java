package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.TimestampCompressor;
import cn.tomandersen.timeseries.compression.TimestampDecompressor;
import fi.iki.yak.ts.compression.gorilla.BitInput;

/**
 * <h3>GorillaTimestampDecompressor</h3>
 * The decompressor of Gorilla timestamp compression.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/27
 */
public class GorillaTimestampDecompressor extends TimestampDecompressor {

    private long prevTimestamp = 0;
    private long prevDelta = 0;

    private boolean isClosed = false;

    public GorillaTimestampDecompressor(BitInput input) {
        super(input);
    }

    /**
     * Decompress and get a timestamp from the buffer as long type.
     */
    @Override
    public long nextTimestamp() {
        // If reach the end of the buffer, return end sign.
        if (isClosed) return TimestampDecompressor.END_SIGN;
        // Read timestamp control bits.
        int controlBits = input.nextClearBit(4);
        long deltaOfDelta;

        switch (controlBits) {
            case 0b0:
                // '0' bit (i.e. previous and current timestamp interval(delta) is same).
                prevTimestamp = prevDelta + prevTimestamp;
                return prevTimestamp;

            case 0b10:
                // '10' bits (i.e. deltaOfDelta value encoded by zigzag32 is stored input next 7 bits).
                deltaOfDelta = input.getLong(7);
                break;
            case 0b110:
                // '110' bits (i.e. deltaOfDelta value encoded by zigzag32 is stored input next 9 bits).
                deltaOfDelta = input.getLong(9);
                break;
            case 0b1110:
                // '1110' bits (i.e. deltaOfDelta value encoded by zigzag32 is stored input next 12 bits).
                deltaOfDelta = input.getLong(12);
                break;
            case 0b1111:
                // '1111' bits (i.e. deltaOfDelta value encoded by zigzag32 is stored input next 32 bits).
                deltaOfDelta = input.getLong(32);
                // If current deltaOfDelta value is the special end sign, set the isClosed value to true
                // (i.e. this buffer reach the end).
                if ((int) deltaOfDelta == TimestampCompressor.END_SIGN) {
                    // End sign of block
                    isClosed = true;
                    return TimestampDecompressor.END_SIGN;
                }
                break;
            default:
                // If unexpected situation occurred, close the block and stop the decompression.
                isClosed = true;
                return TimestampDecompressor.END_SIGN;
        }

        // Decode the deltaOfDelta value.
        deltaOfDelta = decodeZigZag32((int) deltaOfDelta);
        // Since we have decreased the 'delta-of-delta' by 1 when we compress the it,
        // we restore it's value here.
        if (deltaOfDelta >= 0) deltaOfDelta++;
        // Calculate the new delta and timestamp.
        prevDelta += deltaOfDelta;
        prevTimestamp += prevDelta;

        return prevTimestamp;
    }


}
