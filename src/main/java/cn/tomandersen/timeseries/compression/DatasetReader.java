package cn.tomandersen.timeseries.compression;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * <h3>DatasetReader</h3>
 * Only used for read time-series dataset file with specific format.
 * e.g.
 * 430737
 * 1523075452 1
 * 1523075454 1
 * 1523075456 1
 * 1523075458 1
 * ...
 * PS: The first value is the number of data point in this file.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/3
 */
public class DatasetReader {

    private static ByteBuffer timestampBuffer;
    private static ByteBuffer valueBuffer;

    private static String cacheLongValueFilePath = ""; // Cache the last read filename avoid repeat reading.
    private static String cacheDoubleValueFilePath = ""; // Cache the last read filename avoid repeat reading.

    private DatasetReader() {
    }

    /**
     * Read specific dataset file into a entire buffer, and return the buffer.
     *
     * @return byte buffer of the dataset.
     */
    @Deprecated
    public static ByteBuffer read(String filename) {
        // Do nothing
        return null;
    }

    /**
     * Read specific dataset file and divide the data into timestamp and metric value buffer.
     * This method read the file by 'Reader' instead of 'InputStream', and parse every row
     * into two long type value.
     */
    public static void readAndDivideLong(String filename) {
        if (cacheLongValueFilePath.equals(filename)) {
            timestampBuffer.rewind();
            valueBuffer.rewind();
            return;
        }
        cacheLongValueFilePath = filename;
        String line = "";
        int capacity; // The number of data point.
        // Read file and get timestamp and metric value line by line.
        try (BufferedReader in = new BufferedReader(new FileReader(filename))) {
            // Get the total number of point
            capacity = Integer.parseInt(in.readLine());

            timestampBuffer = ByteBuffer.allocate(capacity * Long.BYTES);
            valueBuffer = ByteBuffer.allocate(capacity * Long.BYTES);

            // Read lines and divide it into timestamp and metric value.
            while ((line = in.readLine()) != null) {
                String[] strings = line.split("\\s+");
                timestampBuffer.putLong(Long.parseLong(strings[0]));
                try {
                    // If can not parse the string to long type value, parse the string to double
                    // type value, then print the line and continue.
                    valueBuffer.putLong(Long.parseLong(strings[1]));
                } catch (NumberFormatException e) {
                    valueBuffer.putLong(Double.doubleToRawLongBits(Double.parseDouble(strings[1])));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Read specific dataset file and divide the data into timestamp and metric value buffer.
     * This method read the file by 'Reader' instead of 'InputStream', and parse every row
     * into a long type and double type value.
     */
    public static void readAndDivideDouble(String filename) {
        if (cacheDoubleValueFilePath.equals(filename)) {
            timestampBuffer.rewind();
            valueBuffer.rewind();
            return;
        }
        cacheDoubleValueFilePath = filename;
        String line;
        int capacity; // The number of data point.
        // Read file and get timestamp and metric value line by line.
        try (BufferedReader in = new BufferedReader(new FileReader(filename))) {
            // Get the total number of point
            capacity = Integer.parseInt(in.readLine());

            timestampBuffer = ByteBuffer.allocate(capacity * Long.BYTES);
            valueBuffer = ByteBuffer.allocate(capacity * Long.BYTES);

            // Read lines and divide it into timestamp and metric value.
            while ((line = in.readLine()) != null) {
                String[] strings = line.split("\\s+");
                timestampBuffer.putLong(Long.parseLong(strings[0]));
                valueBuffer.putLong(Double.doubleToRawLongBits(Double.parseDouble(strings[1])));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ByteBuffer getTimestampBuffer() {
//        timestampBuffer.flip();
        return timestampBuffer;
    }

    public static ByteBuffer getValueBuffer() {
//        valueBuffer.flip();
        return valueBuffer;
    }

}
