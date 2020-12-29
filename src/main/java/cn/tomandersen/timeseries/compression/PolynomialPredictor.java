package cn.tomandersen.timeseries.compression;

/**
 * <h3>PolynomialPredictor</h3>
 * Used in original gorilla compression/decompression algorithm.
 * Just take previous metric value as next predicted value.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/4
 */
public class PolynomialPredictor implements Predictor {

    public enum MODE {
        ONE, TWO, THREE, FOUR
    }

    private long prevValue = 0;

    public PolynomialPredictor() {
    }

    /**
     * Input a sampling(actual) value and update the predictor.
     *
     * @param value next sampling(actual) value.
     */
    @Override
    public void update(long value) {

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

    /**
     * Predicts the next value using the default mode, and return
     * the predicted value in long type.
     *
     * @return predicted value.
     */
    @Override
    public long predict() {
        return 0;
    }

    /**
     * Predict next value based using the specific mode, and return
     * the predicted value in long type.
     *
     * @param mode mode used to predict.
     * @return predicted value.
     */
    public long predict(MODE mode) {

        return 0;
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
