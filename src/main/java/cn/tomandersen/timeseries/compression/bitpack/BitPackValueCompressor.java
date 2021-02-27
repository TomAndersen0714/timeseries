package cn.tomandersen.timeseries.compression.bitpack;

import cn.tomandersen.timeseries.compression.BitWriter;
import cn.tomandersen.timeseries.compression.MetricValueCompressor;

/**
 * <h3>BitPackValueCompressor</h3>
 * Used for integer type value compression with BitPack Algorithm.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/17
 * @see BitPackValueDecompressor
 */
public class BitPackValueCompressor extends MetricValueCompressor {

    static final int DEFAULT_FRAME_SIZE = 8;

    private long[] frame;
    private int pos = 0; // position for next element in current frame.

    private int maxLeastSignificantBits = 0;


    public BitPackValueCompressor(BitWriter output) {
        super(output);
        frame = new long[DEFAULT_FRAME_SIZE];
    }

    public BitPackValueCompressor(BitWriter output, int frameSize) {
        super(output);
        frame = new long[frameSize];
    }

    /**
     * Compress a value into specific buffer.
     *
     * @param value value to compress.
     */
    @Override
    public void addValue(long value) {
        // If current frame is full, then flush it.
        if (pos >= frame.length) flush();

        // Calculate the difference between current value and previous value.
        long diff = encodeZigZag64(value - predictor.predict());
        predictor.update(value);

        // Try to update the maximum number of least significant bit.
        maxLeastSignificantBits = Math.max(maxLeastSignificantBits, Long.SIZE - Long.numberOfLeadingZeros(diff));

        // Store value into the current frame.
        frame[pos++] = diff;
    }

    /**
     * Write the frame into buffer and flush it.
     */
    private void flush() {
        // Since the range of value in 6 bits is [0~63], we need to combine the situation when
        // 'maxLeastSignificantBits' equals to 63 or 64
        if (maxLeastSignificantBits == 63) maxLeastSignificantBits++;
        if (maxLeastSignificantBits == 64) {
            // Write the minimum leading zero into buffer as the header of current frame.
            output.writeBits(maxLeastSignificantBits - 1, 6);
        }
        else
            // Write the minimum leading zero into buffer as the header of current frame.
            output.writeBits(maxLeastSignificantBits, 6);


        // Write the significant bits of every value in current frame into buffer.
        for (int i = 0; i < pos; i++) {
            output.writeBits(frame[i], maxLeastSignificantBits);
        }
        // Reset the pos and the maximum number of least significant bit in the frame.
        pos = 0;
        maxLeastSignificantBits = 0;
    }

    /**
     * Close the buffer and stop compression.
     */
    @Override
    public void close() {
        isClosed = true;
        // Flush the left value in current frame.
        if (pos != 0) flush();

        // Write the cached byte(s) to the buffer.
        output.flush();
    }

}
