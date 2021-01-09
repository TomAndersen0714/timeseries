package cn.tomandersen.timeseries.compression.bucket;

import cn.tomandersen.timeseries.compression.BitReader;
import cn.tomandersen.timeseries.compression.MetricValueDecompressor;
import cn.tomandersen.timeseries.compression.predictor.Predictor;

/**
 * BucketValueDecompressor
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2021/1/9
 */
public class BucketValueDecompressor extends MetricValueDecompressor {

    protected BucketValueDecompressor(BitReader input) {
        super(input);
    }

    protected BucketValueDecompressor(BitReader input, Predictor predictor) {
        super(input, predictor);
    }

    /**
     * Decompress a long value from the specific buffer stream.
     *
     * @return decompressed value input long type.
     */
    @Override
    public long nextValue() {
        return 0;
    }
}
