package cn.tomandersen.timeseries.compression;

public interface Predictor {

    /**
     * Input a sampling(actual) value and update the predictor.
     *
     * @param value next sampling(actual) value.
     */
    void update(long value);

    /**
     * Predicts the next value based on the previous values, and
     * return all predicted values for next sampling value by a
     * long array.
     *
     * @return all predicted values.
     */
    long[] predictAll();

    /**
     * Predicts the next value based on the default mode, and return
     * the predicted value in long type.
     *
     * @return predicted value.
     */
    long predict();

    /**
     * Calculate all residual between sampling value and all predicted
     * values, and return the minimum residual.
     *
     * @param sampling the sampling value.
     * @return minimum residual between sampling value and predicted values.
     */
    long residual(long sampling);
}
