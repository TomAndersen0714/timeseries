package cn.tomandersen.timeseries.compression.APE;

import cn.tomandersen.timeseries.compression.benchmark.APECompressionDemo;
import cn.tomandersen.timeseries.compression.BitWriter;
import cn.tomandersen.timeseries.compression.TimestampCompressor;

/**
 * <h3>RLETimestampCompressor</h3>
 * Difference from {@link APETimestampCompressor} is about the control bits and the size of the each bucket.
 * This class have used shorter control bits and smaller bucket for better compression ratio.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 * @see RLETimestampDecompressor
 */
public class RLETimestampCompressor extends TimestampCompressor {

    private long prevTimestamp = -1;
    private int prevDelta = 0;
    private int storedZeros = 0;

    private static final int DELTA_3_MASK = 0b10 << 3;
    private static final int DELTA_5_MASK = 0b110 << 5;
    private static final int DELTA_9_MASK = 0b1110 << 9;

//    private static final int DELTA_7_MASK = 0b10 << 7;
//    private static final int DELTA_9_MASK = 0b110 << 9;
//    private static final int DELTA_12_MASK = 0b1110 << 12;

    public RLETimestampCompressor(BitWriter output) {
        super(output);
    }

    /**
     * Compress a timestamp into specific {@link BitWriter buffer stream}.
     *
     * @param timestamp unix timestamp in second or millisecond.
     */
    @Override
    public void addTimestamp(long timestamp) {
        // Write the header of current block for supporting millisecond.
        if (prevTimestamp < 0) {
            prevTimestamp = timestamp;
            output.writeLong(timestamp);
            return;
        }

        // Calculate the delta of delta.
        int newDelta = (int) (timestamp - prevTimestamp);
        int deltaOfDelta = newDelta - prevDelta;

        if (deltaOfDelta == 0) {
            // Write '0' bit as control bit(i.e. previous and current delta value is same).
//            output.writeZeroBit();
            storedZeros++;
            APECompressionDemo.a0++;
        }
        else {
            // Write the stored zeros to the buffer.
            flushZeros();
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
                    output.writeBits(deltaOfDelta | DELTA_3_MASK, 5);
                    APECompressionDemo.a1++;
                    break;
                case 4:
                case 5:
                    output.writeBits(deltaOfDelta | DELTA_5_MASK, 8);
                    APECompressionDemo.a2++;
                    break;
                case 6:
                case 7:
//                    output.writeBits(deltaOfDelta | DELTA_7_MASK, 9);
//                    break;
                case 8:
                case 9:
//                    output.writeBits(deltaOfDelta | DELTA_9_MASK, 12);
                    output.writeBits(deltaOfDelta | DELTA_9_MASK, 13);
                    APECompressionDemo.a3++;
                    break;
                case 10:
                case 11:
                case 12:
//                    output.writeBits(deltaOfDelta | DELTA_12_MASK, 16);
//                    break;
                default:
                    output.writeBits(0b1111, 4); // Write '1111' control bits.
                    // Since it only takes 4 bytes(i.e. 32 bits) to save a unix timestamp input second, we write
                    // delta-of-delta using 32 bits.
                    output.writeBits(deltaOfDelta, 32);

                    APECompressionDemo.a4++;
                    break;
            }
            prevDelta = newDelta;
        }
        prevTimestamp = timestamp;
    }

    /**
     * Close the buffer and write special value into buffer as end sign.
     */
    @Override
    public void close() {
        // Write all stored zeros into the buffer.
        flushZeros();

        isClosed = true;
        // Write a special timestamp encoded by zigzag32 as the end sign of this block.
        output.writeBits(0b1111, 4);
//        output.writeBits(0xFFFFFFFF, 32);
        output.writeBits(TimestampCompressor.END_SIGN, 32);
        // Flushes the current byte to the buffer.
        output.flush();
    }

    /**
     * Write all stored zeros into the buffer.
     */
    private void flushZeros() {
        while (storedZeros > 0) {
            // Tips: since storedZeros == 0 is unoccupied, we can utilize it to cover a larger range.
            storedZeros--;

            // Write '0' control bit
            output.writeZeroBit();
            if (storedZeros < 8) {
                // Tips: if there is too much case, you can use the number of leading zeros as
                // the condition for using switch-case code block.

                // Write '0' control bit
                output.writeZeroBit();
                // Write the number of cached zeros
                output.writeBits(storedZeros, 3);
                storedZeros = 0;
            }
            else if (storedZeros < 32) {
                // Write '1' control bit
                output.writeOneBit();
                // Write the number of cached zeros
                output.writeBits(storedZeros, 5);
                storedZeros = 0;
            }
            else {
                // Write '1' control bit
                output.writeOneBit();
                // Write 32 cached zeros
                output.writeBits(0b11111, 5);
                storedZeros -= 31;
            }
        }
    }

}
