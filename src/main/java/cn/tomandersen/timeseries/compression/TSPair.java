package cn.tomandersen.timeseries.compression;

/**
 * <h3>TSPair</h3>
 * Time series pair which consist of timestamp and value.
 * This class built with simple structure and value type to avoid automatic packing.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/1
 */
public class TSPair {
    private final long timestamp;
    private final long value;

    public TSPair(long timestamp, long value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public TSPair(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = Double.doubleToRawLongBits(value);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return timestamp + " " + value;
    }

    @Override
    public int hashCode() {
        return (int) (timestamp + value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj instanceof TSPair) {
            TSPair pair = (TSPair) obj;
            return pair.timestamp == this.timestamp && pair.value == this.value;
        }
        return false;
    }

}
