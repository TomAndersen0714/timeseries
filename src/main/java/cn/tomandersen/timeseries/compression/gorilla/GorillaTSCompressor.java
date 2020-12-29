package cn.tomandersen.timeseries.compression.gorilla;

import cn.tomandersen.timeseries.compression.TimeSeriesCompressor;
import fi.iki.yak.ts.compression.gorilla.BitOutput;

import java.nio.ByteBuffer;

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

    public GorillaTSCompressor(BitOutput output) {
        super(
                new GorillaTimestampCompressor(output),
                new GorillaValueCompressor(output),
                output
        );
    }

    public GorillaTSCompressor(BitOutput timestampOutput, BitOutput valueOutput
    ) {
        super(
                new GorillaTimestampCompressor(timestampOutput),
                new GorillaValueCompressor(valueOutput)
        );
    }

}
