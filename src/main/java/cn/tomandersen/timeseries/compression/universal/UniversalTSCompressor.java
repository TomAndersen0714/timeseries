package cn.tomandersen.timeseries.compression.universal;

import cn.tomandersen.timeseries.compression.BitWriter;
import cn.tomandersen.timeseries.compression.MetricValueCompressor;
import cn.tomandersen.timeseries.compression.TimeSeriesCompressor;
import cn.tomandersen.timeseries.compression.TimestampCompressor;

/**
 * <h3>Universal Time Series Compressor</h3>
 * Support construct time series compressor using any combination of timestamp compressor and
 * metric value compressor.
 * You need to define timestamp compressor and value compressor first, before constructing the
 * universal time series compressor.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/11
 * @see UniversalTSDecompressor
 */
public class UniversalTSCompressor extends TimeSeriesCompressor {
    public UniversalTSCompressor(
            TimestampCompressor tsCompressor,
            MetricValueCompressor valueCompressor,
            BitWriter output
    ) {
        super(tsCompressor, valueCompressor, output);
    }

    public UniversalTSCompressor(
            TimestampCompressor timestampCompressor,
            MetricValueCompressor valueCompressor
    ) {
        super(timestampCompressor, valueCompressor);
    }

    public UniversalTSCompressor(
            Class<? extends TimestampCompressor> timestampCompressorCls,
            Class<? extends MetricValueCompressor> metricValueCompressorCls,
            boolean isSeparate
    ) throws Exception {
        super(timestampCompressorCls, metricValueCompressorCls, isSeparate);
    }
}
