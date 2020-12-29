package cn.tomandersen.timeseries.compression.predictor;

/**
 * Last-Value predictor, a computational predictor using previous value as a prediction for the next one
 *
 * @author Michael Burman
 */
public class LastValuePredictor implements Predictor {
    private long storedVal = 0;

    public LastValuePredictor() {}

    public void update(long value) {
        this.storedVal = value;
    }

    /**
     * Predicts the next value based on the previous values, and
     * return all predicted values for next sampling value by a
     * long array.
     *
     * @return all predicted values.
     */
    @Override
    public long[] predictAll() {
        return new long[0];
    }

    public long predict() {
        return storedVal;
    }

    /**
     * Calculate all residual between sampling value and all predicted
     * values, and return the minimum residual.
     *
     * @param sampling the sampling value.
     * @return minimum residual between sampling value and predicted values.
     */
    @Override
    public long residual(long sampling) {
        return 0;
    }
}
