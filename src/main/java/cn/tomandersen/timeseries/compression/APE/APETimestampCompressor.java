package cn.tomandersen.timeseries.compression.APE;

import cn.tomandersen.timeseries.compression.TimestampCompressor;
import fi.iki.yak.ts.compression.gorilla.BitOutput;

/**
 * <h3>APETimestampCompressor</h3>
 * Difference from GorillaTimestampCompressor is that APETimestampCompressor have used the RLE coding.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/4
 */
public class APETimestampCompressor extends TimestampCompressor {

    private long prevTimestamp = 0;
    private int prevDelta = 0;
    private int storedZeros = 0;

    private static final int MASK_OFFSET_7 = 0b10 << 7;
    private static final int MASK_OFFSET_9 = 0b110 << 9;
    private static final int MASK_OFFSET_12 = 0b1110 << 12;

    public APETimestampCompressor(BitOutput output) {
        super(output);
    }

    /**
     * Compress a timestamp into specific {@link BitOutput buffer stream}.
     *
     * @param timestamp unix timestamp in second.
     */
    @Override
    public void addTimestamp(long timestamp) {

        // Calculate the delta of delta
        int newDelta = (int) (timestamp - prevTimestamp);
        int deltaOfDelta = newDelta - prevDelta;

        if (deltaOfDelta == 0) {
            // Write '0' bit as control bit(i.e. previous and current delta value is same).
//            output.skipBit();
            storedZeros++;

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
                case 4:
                case 5:
                case 6:
                case 7:
                    output.writeBits(deltaOfDelta | MASK_OFFSET_7, 9);
                    break;
                case 8:
                case 9:
                    output.writeBits(deltaOfDelta | MASK_OFFSET_9, 12);
                    break;
                case 10:
                case 11:
                case 12:
                    output.writeBits(deltaOfDelta | MASK_OFFSET_12, 16);
                    break;
                default:
                    output.writeBits(0b1111, 4); // Write '1111' control bits.
                    // Since it only takes 4 bytes(i.e. 32 bits) to save a unix timestamp input second, we write
                    // delta-of-delta using 32 bits.
                    output.writeBits(deltaOfDelta, 32);
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
            output.skipBit();
            if (storedZeros < 8) {
                // Tips: if there is too much case, you can use the number of leading zeros as
                // the condition for using switch-case code block.

                // Write '0' control bit
                output.skipBit();
                // Write the number of cached zeros
                output.writeBits(storedZeros, 3);
                storedZeros = 0;
            }
            else if (storedZeros < 32) {
                // Write '1' control bit
                output.writeBit();
                // Write the number of cached zeros
                output.writeBits(storedZeros, 5);
                storedZeros = 0;
            }
            else {
                // Write '1' control bit
                output.writeBit();
                // Write 32 cached zeros
                output.writeBits(0b11111, 5);
                storedZeros -= 31;
            }
        }
    }

}
