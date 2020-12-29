package cn.tomandersen.timeseries.compression;

import fi.iki.yak.ts.compression.gorilla.BitInput;
import fi.iki.yak.ts.compression.gorilla.BitOutput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

/**
 * <h3>TimeSeriesCompressor</h3>
 * Single-use time-series compressor.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/1
 * @see TimestampCompressor
 * @see MetricValueCompressor
 */
public abstract class TimeSeriesCompressor {

    private final TimestampCompressor timestampCompressor;
    private final MetricValueCompressor valueCompressor;

    private final boolean isSeparate;
    private BitOutput out;

    /**
     * Compress the time-series into single stream.
     */
    public TimeSeriesCompressor(
            TimestampCompressor timestampCompressor,
            MetricValueCompressor valueCompressor,
            BitOutput output
    ) {
        this.timestampCompressor = timestampCompressor;
        this.valueCompressor = valueCompressor;
        this.out = output;
        this.isSeparate = false;
    }

    /**
     * Compress the time-series into timestamp stream and value stream separately.
     */
    public TimeSeriesCompressor(
            TimestampCompressor timestampCompressor,
            MetricValueCompressor valueCompressor
    ) {
        this.timestampCompressor = timestampCompressor;
        this.valueCompressor = valueCompressor;
        this.isSeparate = true;
    }

    /**
     * Construct time-series compressor by specific compressor.
     *
     * @param timestampCompressorCls   the Class object of timestamp compressor.
     * @param metricValueCompressorCls the Class object of value compressor.
     * @param isSeparate               whether to compress separately.
     */
    public TimeSeriesCompressor(
            Class<? extends TimestampCompressor> timestampCompressorCls,
            Class<? extends MetricValueCompressor> metricValueCompressorCls,
            boolean isSeparate
    ) throws Exception {
        Constructor<? extends TimestampCompressor>
                timestampCompressorConstructor = timestampCompressorCls.getConstructor(BitOutput.class);
        Constructor<? extends MetricValueCompressor>
                valueCompressorConstructor = metricValueCompressorCls.getConstructor(BitOutput.class);
        if (isSeparate) {
            ByteBufferBitOutput output = new ByteBufferBitOutput();
            this.timestampCompressor = timestampCompressorConstructor.newInstance(output);
            this.valueCompressor = valueCompressorConstructor.newInstance(output);
        }
        else {
            ByteBufferBitOutput timestampOutput = new ByteBufferBitOutput();
            ByteBufferBitOutput valueOutput = new ByteBufferBitOutput();
            this.timestampCompressor = timestampCompressorConstructor.newInstance(timestampOutput);
            this.valueCompressor = valueCompressorConstructor.newInstance(valueOutput);
        }
        this.isSeparate = isSeparate;
    }

    /**
     * Compress a timestamp-value pair.
     */
    public void addPair(long timestamp, long value) {
        timestampCompressor.addTimestamp(timestamp);
        valueCompressor.addValue(value);
    }

    /**
     * Compress a timestamp-value pair.
     */
    public void addPair(long timestamp, double value) {
        timestampCompressor.addTimestamp(timestamp);
        valueCompressor.addValue(value);
    }

    /**
     * Compress a long type timestamp.
     */
    public void addTimestamp(long timestamp) {
        timestampCompressor.addTimestamp(timestamp);
    }


    /**
     * Compress a long type metric value.
     */
    public void addValue(long value) {
        valueCompressor.addValue(value);
    }


    /**
     * Compress a double type metric value.
     */
    public void addValue(double value) {
        valueCompressor.addValue(value);
    }

    /**
     * Compress a file.
     *
     * @param filename The name of the file to be compressed.
     */
    public void compress(String filename, boolean isDoubleValue) {
        // Read specific dataset file and divide the data into timestamp and metric value buffer.
        if (isDoubleValue)
            DatasetReader.readAndDivideDouble(filename);
        else
            DatasetReader.readAndDivideLong(filename);
        // Compress the uncompressed buffer.
        compress(DatasetReader.getTimestampBuffer(), DatasetReader.getValueBuffer());
    }


    /**
     * Compress a time series buffer.
     */
    public void compress(ByteBuffer timeSeriesBuffer) {
        // Create a new buffer reference to the same uncompressed data.
        ByteBuffer uncompressedBuffer = timeSeriesBuffer.duplicate();
        uncompressedBuffer.rewind();
        // Compress every pair in the uncompressed buffer.
        while (uncompressedBuffer.remaining() >= Long.BYTES * 2) {
            timestampCompressor.addTimestamp(uncompressedBuffer.getLong());
            valueCompressor.addValue(uncompressedBuffer.getLong());
        }
    }

    /**
     * Compress timestamp and metric value separately.
     *
     * @param timestampBuffer timestamp byte buffer to compress.
     * @param valueBuffer     metric value byte buffer to compress.
     */
    public void compress(ByteBuffer timestampBuffer, ByteBuffer valueBuffer) {
//        // Create new buffer reference to the same uncompressed data.
//        ByteBuffer uncompressedTimestamp = timestampBuffer.duplicate();
//        ByteBuffer uncompressedValue = valueBuffer.duplicate();
//        // Reset the position of Buffer.
//        uncompressedTimestamp.rewind();
//        uncompressedValue.rewind();

        // Reset the position of Buffer.
        timestampBuffer.rewind();
        valueBuffer.rewind();

        // Compress every time-series pair in the uncompressed buffer.
        while (timestampBuffer.remaining() >= Long.BYTES
                && valueBuffer.remaining() >= Long.BYTES) {
            timestampCompressor.addTimestamp(timestampBuffer.getLong());
            valueCompressor.addValue(valueBuffer.getLong());
        }
    }

    /**
     * Close the block and flush the remaining stuff of current byte to the buffer.
     */
    public void close() {
        // Close the timestamp compression stream.
        timestampCompressor.close();
        // Close the value compression stream.
        valueCompressor.close();

        // Write the remaining bits into output buffer for byte-alignment.
        if (isSeparate && getTimestampOutput() != null && getValueOutput() != null) {
            getTimestampOutput().flush();
            getValueOutput().flush();
        }
        else if (!isSeparate && getOutput() != null) {
            getOutput().flush();
        }
    }

    public BitOutput getTimestampOutput() {
        return timestampCompressor.getOutput();
    }

    public BitOutput getValueOutput() {
        return valueCompressor.getOutput();
    }

    public boolean isSeparate() {
        return isSeparate;
    }

    public BitOutput getOutput() {
        return out;
    }
}
