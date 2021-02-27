package cn.tomandersen.timeseries.compression.bitpack;

import cn.tomandersen.timeseries.compression.BitReader;
import cn.tomandersen.timeseries.compression.MetricValueDecompressor;
import cn.tomandersen.timeseries.compression.predictor.Predictor;

/**
 * <h3>BitPackValueDecompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/18
 * @see BitPackValueCompressor
 */
public class BitPackValueDecompressor extends MetricValueDecompressor {

    private int pos; // position for next element in current frame.
    private int capacity; // Capacity of current frame.
    private int maxLeastSignificantBits = 0;


    public BitPackValueDecompressor(BitReader input) {
        super(input);
        this.capacity = BitPackValueCompressor.DEFAULT_FRAME_SIZE;
        this.pos = this.capacity;
    }

    public BitPackValueDecompressor(BitReader input, int capacity) {
        super(input);
        this.capacity = capacity;
        this.pos = this.capacity;
    }

    public BitPackValueDecompressor(BitReader input, Predictor predictor) {
        super(input, predictor);
        this.capacity = BitPackValueCompressor.DEFAULT_FRAME_SIZE;
        this.pos = this.capacity;
    }

    public BitPackValueDecompressor(BitReader input, Predictor predictor, int capacity) {
        super(input, predictor);
        this.capacity = capacity;
        this.pos = this.capacity;
    }

    /**
     * Decompress a long value from the specific buffer stream.
     *
     * @return decompressed value input long type.
     */
    @Override
    public long nextValue() {
        // If current compressed frame reach the end, read next maximum number of least
        // significant bit.
        if (pos == capacity) {
            maxLeastSignificantBits = (int) input.nextLong(6);
            pos = 0;
        }
        // Handle the situation when 'maxLeastSignificantBits' equals to 63
        // Since we combine the situation when 'maxLeastSignificantBits' equals to 63 or 64, so
        // we need to handle it as same situation.
        if (maxLeastSignificantBits == 63) maxLeastSignificantBits++;

        // Decompress the difference in current frame according to the value of maxLeastSignificantBits
        long diff = decodeZigZag64(input.nextLong(maxLeastSignificantBits));
        // Restore the value.
        long value = diff + predictor.predict();
        // update predictor and position.
        predictor.update(value);
        pos++;
        // Return current value.
        return value;
    }
}
