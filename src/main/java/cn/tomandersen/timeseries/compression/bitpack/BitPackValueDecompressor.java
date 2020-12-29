package cn.tomandersen.timeseries.compression.bitpack;

import cn.tomandersen.timeseries.compression.MetricValueDecompressor;
import fi.iki.yak.ts.compression.gorilla.BitInput;
import fi.iki.yak.ts.compression.gorilla.Predictor;

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


    public BitPackValueDecompressor(BitInput input) {
        super(input);
        this.capacity = BitPackValueCompressor.DEFAULT_FRAME_SIZE;
        this.pos = this.capacity;
    }

    public BitPackValueDecompressor(BitInput input, int capacity) {
        super(input);
        this.capacity = capacity;
        this.pos = this.capacity;
    }

    public BitPackValueDecompressor(BitInput input, Predictor predictor) {
        super(input, predictor);
        this.capacity = BitPackValueCompressor.DEFAULT_FRAME_SIZE;
        this.pos = this.capacity;
    }

    public BitPackValueDecompressor(BitInput input, Predictor predictor, int capacity) {
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
            maxLeastSignificantBits = (int) input.getLong(6);
            pos = 0;
        }
        // Decompress the difference in current frame according to the value of maxLeastSignificantBits
        long diff = decodeZigZag64(input.getLong(maxLeastSignificantBits));
        // Restore the value.
        long value = diff + predictor.predict();
        // update predictor and position.
        predictor.update(value);
        pos++;
        // Return current value.
        return value;
    }
}
