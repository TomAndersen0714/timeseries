package cn.tomandersen.timeseries.compression.universal;

import cn.tomandersen.timeseries.compression.*;

import java.nio.ByteBuffer;

/**
 * <h3>Universal Time Series Decompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/11
 * @see UniversalTSCompressor
 */
public class UniversalTSDecompressor extends TimeSeriesDecompressor {

    /**
     * Only support decompression by {@link #nextPair}.
     */
    public UniversalTSDecompressor(
            TimestampDecompressor timestampDecompressor, MetricValueDecompressor valueDecompressor
    ) {
        super(timestampDecompressor, valueDecompressor);
    }

    /**
     * Only support decompression by {@link #decompress}.
     */
    public UniversalTSDecompressor(
            TimestampDecompressor timestampDecompressor, MetricValueDecompressor valueDecompressor,
            ByteBuffer decompressedOutputBuffer
    ) {
        super(timestampDecompressor, valueDecompressor, decompressedOutputBuffer);
    }

    /**
     * Only support decompression by {@link #decompress}.
     */
    public UniversalTSDecompressor(
            TimestampDecompressor timestampDecompressor, MetricValueDecompressor valueDecompressor,
            ByteBuffer decompressedTimestamp, ByteBuffer decompressedValue
    ) {
        super(timestampDecompressor, valueDecompressor, decompressedTimestamp, decompressedValue);
    }


}
