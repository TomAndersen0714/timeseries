package cn.tomandersen.timeseries.compression;

import fi.iki.yak.ts.compression.gorilla.Predictor;
import fi.iki.yak.ts.compression.gorilla.ValueDecompressor;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

/**
 * <h3>TimeSeriesDecompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/1
 * @see TimeSeriesCompressor
 */
public abstract class TimeSeriesDecompressor {

    private TimestampDecompressor timestampDecompressor;
    private MetricValueDecompressor valueDecompressor;

    private ByteBuffer decompressedTimestampBuffer;
    private ByteBuffer decompressedValueBuffer;
    private ByteBuffer decompressedOutputBuffer;

    protected final boolean isSeparate;

    /**
     * Only support decompression by {@link #nextPair}.
     */
    protected TimeSeriesDecompressor(
            TimestampDecompressor timestampDecompressor,
            MetricValueDecompressor valueDecompressor
    ) {
        this.timestampDecompressor = timestampDecompressor;
        this.valueDecompressor = valueDecompressor;
        this.isSeparate = false;
    }

    /**
     * Only support decompression by {@link #decompress}.
     */
    protected TimeSeriesDecompressor(
            TimestampDecompressor timestampDecompressor,
            MetricValueDecompressor valueDecompressor,
            ByteBuffer decompressedOutputBuffer
    ) {
        this.timestampDecompressor = timestampDecompressor;
        this.valueDecompressor = valueDecompressor;
        this.decompressedOutputBuffer = decompressedOutputBuffer;
        this.isSeparate = false;
    }

    /**
     * Only support decompression by {@link #decompress}.
     */
    protected TimeSeriesDecompressor(
            TimestampDecompressor timestampDecompressor,
            MetricValueDecompressor valueDecompressor,
            ByteBuffer decompressedTimestampBuffer,
            ByteBuffer decompressedValueBuffer
    ) {
        this.timestampDecompressor = timestampDecompressor;
        this.valueDecompressor = valueDecompressor;
        this.decompressedTimestampBuffer = decompressedTimestampBuffer;
        this.decompressedValueBuffer = decompressedValueBuffer;
        this.isSeparate = true;
    }


    /**
     * Decompress and get next timestamp-value pair.
     */
    public TSPair nextPair() {
        // Get next timestamp.
        long timestamp = timestampDecompressor.nextTimestamp();
        // If reach the end of the block, return null.
        if (timestamp == TimestampDecompressor.END_SIGN) return null;
        return new TSPair(timestamp, valueDecompressor.nextValue());
    }

    /**
     * Decompress the entire/remaining block into decompressedOutputBuffer buffer.
     * This method support two mode(i.e. separately or not).
     */
    public void decompress() {
        // Decompress next timestamp.
        long timestamp, value;

        if (isSeparate) {
//            ByteBuffer decompressedTimestampBuffer = getDecompressedTimestampBuffer();
//            ByteBuffer decompressedValueBuffer = getDecompressedValueBuffer();
            if (decompressedTimestampBuffer == null || decompressedValueBuffer == null) return;
            // Decompress the timestamp and metric value and store into respective output buffer,
            // until reach the end of the block.
            while ((timestamp = timestampDecompressor.nextTimestamp())
                    != TimestampDecompressor.END_SIGN) {
                try {
                    value = valueDecompressor.nextValue();
                } catch (Exception e) {
                    value = 0;
                }
                decompressedTimestampBuffer.putLong(timestamp);
                decompressedValueBuffer.putLong(value);
            }
            // Switch buffer from 'write' mode to 'read' mode.
            decompressedTimestampBuffer.flip();
            decompressedValueBuffer.flip();
        }
        else {
            if (decompressedOutputBuffer == null) return;
            // Decompress the timestamp and metric value and store into output buffer,
            // until reach the end of the block.
            while ((timestamp = timestampDecompressor.nextTimestamp())
                    != TimestampDecompressor.END_SIGN) {
                value = valueDecompressor.nextValue();
                decompressedOutputBuffer.putLong(timestamp);
                decompressedOutputBuffer.putLong(value);
            }
            // Switch buffer from 'write' mode to 'read' mode.
            decompressedOutputBuffer.flip();
        }
    }


    protected ByteBuffer getDecompressedTimestampBuffer() {
        return decompressedTimestampBuffer;
    }

    protected ByteBuffer getDecompressedValueBuffer() {
        return decompressedValueBuffer;
    }

    protected ByteBuffer getDecompressedOutputBuffer() {
        return decompressedOutputBuffer;
    }
}
