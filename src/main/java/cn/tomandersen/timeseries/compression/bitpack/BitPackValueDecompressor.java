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
        long diff = 0, value = 0;
        // If current compressed frame reach the end, read next maximum number of least
        // significant bit.
        if (pos == capacity) {
            maxLeastSignificantBits = (int) input.nextLong(6);
            pos = 0;
        }
        // If maxLeastSignificantBits equals zero, the all diff value in
        // current frame is zero.(i.e. current value and previous is same)
        if (maxLeastSignificantBits == 0) {
            // Restore the value.
            value = diff + predictor.predict();
        }
        else {
            // Decompress the difference in current frame according to the value of maxLeastSignificantBits
            // Since we compressed 'maxLeastSignificantBits-1' into buffer,
            // we restore it here
            diff = decodeZigZag64(input.nextLong(maxLeastSignificantBits + 1));
            // Restore the value.
            value = diff + predictor.predict();

        }
        // update predictor and position.
        predictor.update(value);
        pos++;
        // Return current value.
        return value;
    }
}
