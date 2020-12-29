package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.TimestampCompressor;
import cn.tomandersen.timeseries.compression.gorilla.demos.GorillaCompressionDemo;
import fi.iki.yak.ts.compression.gorilla.BitOutput;

/**
 * <h3>GorillaTimestampCompressor</h3>
 * This Gorilla timestamp compressor only support compressing unix timestamp sequence input second.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/26
 */
public class GorillaTimestampCompressor extends TimestampCompressor {

    private long prevTimestamp = 0;
    private int prevDelta = 0;

    private static final int DELTA_7_MASK = 0b10 << 7;
    private static final int DELTA_9_MASK = 0b110 << 9;
    private static final int DELTA_12_MASK = 0b1110 << 12;

    public GorillaTimestampCompressor(BitOutput output) {
        super(output);
    }

    /**
     * Compress a timestamp into specific {@link BitOutput buffer stream}.
     *
     * @param timestamp Unix timestamp input second.
     */
    public void addTimestamp(long timestamp) {

        // Calculate the delta of delta
        int newDelta = (int) (timestamp - prevTimestamp);
        int deltaOfDelta = newDelta - prevDelta;

        if (deltaOfDelta == 0) {
            // Write '0' bit as control bit(i.e. previous and current delta value is same).
            GorillaCompressionDemo.a0++;
            output.skipBit();
        }
        else {
            // Tips: since deltaOfDelta == 0 is unoccupied, we can utilize it to cover a larger range.
            if (deltaOfDelta > 0) deltaOfDelta--;
            // Convert signed value to unsigned value for compression.
            deltaOfDelta = encodeZigZag32(deltaOfDelta);

            int leastBitLength = Integer.SIZE - Integer.numberOfLeadingZeros(deltaOfDelta);
            // Match the deltaOfDelta to the three case as follow.
            switch (leastBitLength) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    output.writeBits(deltaOfDelta | DELTA_7_MASK, 9);
                    GorillaCompressionDemo.a1++;
                    break;
                case 8:
                case 9:
                    output.writeBits(deltaOfDelta | DELTA_9_MASK, 12);
                    GorillaCompressionDemo.a2++;
                    break;
                case 10:
                case 11:
                case 12:
                    output.writeBits(deltaOfDelta | DELTA_12_MASK, 16);
                    GorillaCompressionDemo.a3++;
                    break;
                default:
                    output.writeBits(0b1111, 4); // Write '1111' control bits.
                    // Since it only takes 4 bytes(i.e. 32 bits) to save a unix timestamp input second, we write
                    // delta-of-delta using 32 bits.
                    output.writeBits(deltaOfDelta, 32);
                    GorillaCompressionDemo.a4++;
                    break;
            }
            prevDelta = newDelta;
        }
        prevTimestamp = timestamp;
    }

    /**
     * Close the buffer and write special value into buffer as end sign.
     */
    public void close() {
        isClosed = true;
        // Write a special timestamp encoded by zigzag32 as the end sign of this block.
        output.writeBits(0b1111, 4);
//        output.writeBits(0xFFFFFFFF, 32);
        output.writeBits(TimestampCompressor.END_SIGN, 32);
        // Flushes the current byte to the buffer.
        output.flush();
    }

}
