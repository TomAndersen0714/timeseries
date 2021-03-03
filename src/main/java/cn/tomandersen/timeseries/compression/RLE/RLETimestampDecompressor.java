package cn.tomandersen.timeseries.compression.RLE;

import cn.tomandersen.timeseries.compression.BitReader;
import cn.tomandersen.timeseries.compression.TimestampCompressor;
import cn.tomandersen.timeseries.compression.TimestampDecompressor;

/**
 * <h3>RLETimestampDecompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 */
public class RLETimestampDecompressor extends TimestampDecompressor {

    private long prevTimestamp = -1;
    private long prevDelta = 0;
    private long storedZeros = 0;

    private boolean isClosed = false;


    public RLETimestampDecompressor(BitReader input) {
        super(input);
    }

    /**
     * Decompress and get a timestamp from the buffer as long type.
     */
    @Override
    public long nextTimestamp() {
        // If reach the end of the buffer, return 0L.
        if (isClosed)
            return TimestampDecompressor.END_SIGN;

        // Get the head of current block.
        if (prevTimestamp < 0) {
            prevTimestamp = input.nextLong();
            return prevTimestamp;
        }

        // If storedZeros != 0, previous and current timestamp interval(delta) is same,
        // just update prevTimestamp and storedZeros, and return prevTimestamp.
        if (storedZeros > 0) {
            storedZeros--;
            prevTimestamp = prevDelta + prevTimestamp;
            return prevTimestamp;
        }

        // Read timestamp control bits.
        int controlBits = input.nextControlBits(4);
        long deltaOfDelta;

        switch (controlBits) {
            case 0b0:
                // '0' bit (i.e. previous and current timestamp interval(delta) is same).

                // Get next the number of consecutive zeros
                getConsecutiveZeros();

                storedZeros--;
                prevTimestamp = prevDelta + prevTimestamp;
                return prevTimestamp;

            case 0b10:
                // '10' bits (i.e. deltaOfDelta value encoded by zigzag32 is stored input next 7 bits).
                deltaOfDelta = input.nextLong(3);
                break;
            case 0b110:
                // '110' bits (i.e. deltaOfDelta value encoded by zigzag32 is stored input next 9 bits).
                deltaOfDelta = input.nextLong(5);
                break;
            case 0b1110:
                // '1110' bits (i.e. deltaOfDelta value encoded by zigzag32 is stored input next 12 bits).
                deltaOfDelta = input.nextLong(9);
                break;
            case 0b1111:
                // '1111' bits (i.e. deltaOfDelta value encoded by zigzag32 is stored input next 32 bits).
                deltaOfDelta = input.nextLong(32);
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
        // Since we have decreased the 'delta-of-delta' by 1 when we compress the 'delta-of-delta',
        // we restore the value here.
        if (deltaOfDelta >= 0) deltaOfDelta++;
        // Calculate the new delta and timestamp.
        prevDelta += deltaOfDelta;
        prevTimestamp += prevDelta;

        return prevTimestamp;
    }

    /**
     * Get next the number of consecutive zeros
     */
    private void getConsecutiveZeros() {
        // Read consecutive zeros control bits.
        int controlBits = input.nextControlBits(1);

        switch (controlBits) {
            case 0:
                storedZeros = input.nextLong(3);
                break;
            case 1:
                storedZeros = input.nextLong(5);
                break;
        }
        // Since we have decreased the 'storedZeros' by 1 when we
        // compress it, we restore it's value here.
        storedZeros++;
    }
}
