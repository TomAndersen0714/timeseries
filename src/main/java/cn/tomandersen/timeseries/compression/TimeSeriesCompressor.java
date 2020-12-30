package cn.tomandersen.timeseries.compression;

import cn.tomandersen.timeseries.compression.predictor.Predictor;

import java.lang.reflect.Constructor;
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

    private boolean isSeparate;
    private BitWriter out;

    /**
     * Compress the time-series into single stream.
     */
    public TimeSeriesCompressor(
            TimestampCompressor timestampCompressor,
            MetricValueCompressor valueCompressor,
            BitWriter output
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
     * Construct time-series compressor by specific compressor using default value predictor.
     *
     * @param timestampCompressorCls   the Class object of timestamp compressor.
     * @param metricValueCompressorCls the Class object of value compressor.
     */
    public TimeSeriesCompressor(
            Class<? extends TimestampCompressor> timestampCompressorCls,
            Class<? extends MetricValueCompressor> metricValueCompressorCls
    ) throws Exception {
        this(timestampCompressorCls, metricValueCompressorCls, true);
    }

    /**
     * Construct time-series compressor by specific compressor using default value predictor.
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
                timestampCompressorConstructor = timestampCompressorCls.getConstructor(BitWriter.class);
        Constructor<? extends MetricValueCompressor>
                valueCompressorConstructor = metricValueCompressorCls.getConstructor(BitWriter.class);
        if (isSeparate) {
            BitWriter timestampOutput = new BitBufferWriter();
            BitWriter valueOutput = new BitBufferWriter();
            this.timestampCompressor = timestampCompressorConstructor.newInstance(timestampOutput);
            this.valueCompressor = valueCompressorConstructor.newInstance(valueOutput);
        }
        else {
            BitWriter output = new BitBufferWriter();
            this.timestampCompressor = timestampCompressorConstructor.newInstance(output);
            this.valueCompressor = valueCompressorConstructor.newInstance(output);
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
    public void compress(String filename, boolean isLongOrDoubleValue) {
        // Read specific dataset file and divide the data into timestamp and metric value buffer.
        if (isSeparate) {
            if (isLongOrDoubleValue)
                DatasetReader.readAndDivideLong(filename);
            else
                DatasetReader.readAndDivideDouble(filename);
            // Compress the read buffer.
            compress(DatasetReader.getTimestampBuffer(), DatasetReader.getValueBuffer());
        }
        else {
            compress(DatasetReader.read(filename));
        }
    }


    /**
     * Compress a time series buffer.
     */
    @Deprecated
    public void compress(ByteBuffer timeSeriesBuffer) {
        timeSeriesBuffer.rewind();
        // Compress every pair in the uncompressed buffer.
        while (timeSeriesBuffer.remaining() >= Long.BYTES * 2) {
            timestampCompressor.addTimestamp(timeSeriesBuffer.getLong());
            valueCompressor.addValue(timeSeriesBuffer.getLong());
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
        // Write the remaining bits into output buffer for byte-alignment.
        // Close the timestamp compression stream.
        timestampCompressor.close();
        // Close the value compression stream.
        valueCompressor.close();


//        if (isSeparate && getTimestampOutput() != null && getValueOutput() != null) {
//            getTimestampOutput().flush();
//            getValueOutput().flush();
//        }
//        else if (!isSeparate && getOutput() != null) {
//            getOutput().flush();
//        }
    }

    public BitWriter getTimestampOutput() {
        return timestampCompressor.getOutput();
    }

    public ByteBuffer getCompressedTimestampBuffer() {
        return timestampCompressor.getOutput().getBuffer();
    }

    public BitWriter getValueOutput() {
        return valueCompressor.getOutput();
    }

    public ByteBuffer getCompressedValueBuffer() {
        return valueCompressor.getOutput().getBuffer();
    }

    public boolean isSeparate() {
        return isSeparate;
    }

    public BitWriter getOutput() {
        return out;
    }
}
