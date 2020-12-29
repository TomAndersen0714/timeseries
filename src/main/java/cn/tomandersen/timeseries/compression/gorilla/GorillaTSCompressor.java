package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.BitWriter;
import cn.tomandersen.timeseries.compression.TimeSeriesCompressor;

/**
 * <h3>GorillaTSCompressor</h3>
 * Only support construct Time Series Compressor using {@link GorillaTimestampCompressor}
 * and {@link GorillaValueCompressor}.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/26
 * @see GorillaTSDecompressor
 */
public class GorillaTSCompressor extends TimeSeriesCompressor {

    public GorillaTSCompressor(BitWriter output) {
        super(
                new GorillaTimestampCompressor(output),
                new GorillaValueCompressor(output),
                output
        );
    }

    public GorillaTSCompressor(BitWriter timestampOutput, BitWriter valueOutput
    ) {
        super(
                new GorillaTimestampCompressor(timestampOutput),
                new GorillaValueCompressor(valueOutput)
        );
    }

}
