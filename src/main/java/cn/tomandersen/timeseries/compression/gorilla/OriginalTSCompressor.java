package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.BitWriter;

/**
 * <h3>OriginalTSCompressor</h3>
 *
 * <p>
 * (此处为Class详细描述).
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/12
 * @see OriginalTimestampCompressor
 * @see OriginalValueCompressor
 */
public class OriginalTSCompressor {

    private OriginalTimestampCompressor timestampCompressor;
    private OriginalValueCompressor valueCompressor;

    public final static int FIRST_DELTA_BITS = 27;
    private boolean isFirstTimestamp = true;

    /**
     * Compress the time-series into single stream.
     */
    public OriginalTSCompressor(
            BitWriter output, long blockTimestamp
    ) {
        this.timestampCompressor = new OriginalTimestampCompressor(output);
        this.valueCompressor = new OriginalValueCompressor(output);
        timestampCompressor.addHeader(blockTimestamp);
    }

    /**
     * Compress the time-series into timestamp stream and value stream separately.
     */
    public OriginalTSCompressor(
            BitWriter timestampOutput, BitWriter valueOutput,
            long blockTimestamp
    ) {
        this.timestampCompressor = new OriginalTimestampCompressor(timestampOutput);
        this.valueCompressor = new OriginalValueCompressor(valueOutput);
        timestampCompressor.addHeader(blockTimestamp);
    }

    /**
     * Write first data point into output buffer.
     */
    private void writeFirst(long timestamp, long value) {
        // Write first timestamp delta-of-delta in raw format.
        timestampCompressor.writeFirst(timestamp);
        // Write first metric value in raw format.
        valueCompressor.writeFirst(value);
    }

    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param timestamp Timestamp which is inside the allowed time block (default 24 hours with millisecond precision)
     * @param value     next floating point value in the series
     */
    public void addValue(long timestamp, long value) {
        if (isFirstTimestamp) {
            isFirstTimestamp = false;
            writeFirst(timestamp, value);
        }
        else {
            timestampCompressor.addTimestamp(timestamp);
            valueCompressor.addValue(value);
        }
    }

    /**
     * Adds a new double value to the series. Note, values must be inserted in order.
     *
     * @param timestamp Timestamp which is inside the allowed time block (default 24 hours with millisecond precision)
     * @param value     next floating point value in the series
     */
    public void addValue(long timestamp, double value) {
        if (isFirstTimestamp) {
            isFirstTimestamp = false;
            writeFirst(timestamp, Double.doubleToRawLongBits(value));
            return;
        }
        timestampCompressor.addTimestamp(timestamp);
        valueCompressor.addValue(Double.doubleToRawLongBits(value));
    }

    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    public void close() {
        timestampCompressor.close();
        valueCompressor.close();
    }

}
