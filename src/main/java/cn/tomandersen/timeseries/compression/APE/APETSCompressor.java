package cn.tomandersen.timeseries.compression.APE;

import cn.tomandersen.timeseries.compression.TimeSeriesCompressor;
import fi.iki.yak.ts.compression.gorilla.BitOutput;

/**
 * <h3>APE Time Series Compressor</h3>
 * Only support construct Time Series Compressor using {@link APETimestampCompressor}
 * and {@link APEValueCompressor}.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 * @see APETSDecompressor
 */
public class APETSCompressor extends TimeSeriesCompressor {
    protected APETSCompressor(BitOutput output) {
        super(
                new APETimestampCompressor(output),
                new APEValueCompressor(output),
                output
        );
    }

    public APETSCompressor(BitOutput timestampOutput, BitOutput valueOutput) {
        super(
                new APETimestampCompressor1(timestampOutput),
                new APEValueCompressor2(valueOutput)
        );
    }

}
