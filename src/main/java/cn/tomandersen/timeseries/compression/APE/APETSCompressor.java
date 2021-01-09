package cn.tomandersen.timeseries.compression.APE;

import cn.tomandersen.timeseries.compression.BitWriter;
import cn.tomandersen.timeseries.compression.TimeSeriesCompressor;
import cn.tomandersen.timeseries.compression.bucket.BucketValueCompressor;

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
    protected APETSCompressor(BitWriter output) {
        super(
                new APETimestampCompressor(output),
                new APEValueCompressor(output),
                output
        );
    }

    public APETSCompressor(BitWriter timestampOutput, BitWriter valueOutput) {
        super(
                new RLETimestampCompressor(timestampOutput),
                new BucketValueCompressor(valueOutput)
        );
    }

}
