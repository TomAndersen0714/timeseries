package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.TSPair;
import cn.tomandersen.timeseries.compression.TimeSeriesDecompressor;

import java.nio.ByteBuffer;

/**
 * <h3>GorillaTSDecompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/1
 * @see GorillaTSCompressor
 */
public class GorillaTSDecompressor extends TimeSeriesDecompressor {

    /**
     * Only support decompression by {@link #nextPair}.
     */
    public GorillaTSDecompressor(
            GorillaTimestampDecompressor timestampDecompressor,
            GorillaValueDecompressor valueDecompressor
    ) {
        super(timestampDecompressor, valueDecompressor);
    }


    /**
     * Only support decompression by {@link #decompress }.
     */
    public GorillaTSDecompressor(
            GorillaTimestampDecompressor timestampDecompressor, GorillaValueDecompressor valueDecompressor,
            ByteBuffer timeSeriesOutput
    ) {
        super(timestampDecompressor, valueDecompressor, timeSeriesOutput);
    }

    /**
     * Only support decompression by {@link #decompress }.
     */
    public GorillaTSDecompressor(
            GorillaTimestampDecompressor timestampDecompressor, GorillaValueDecompressor valueDecompressor,
            ByteBuffer timestampsOutput, ByteBuffer valuesOutput
    ) {
        super(timestampDecompressor, valueDecompressor, timestampsOutput, valuesOutput);
    }

}
