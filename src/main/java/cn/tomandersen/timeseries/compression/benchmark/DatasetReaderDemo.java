package cn.tomandersen.timeseries.compression.benchmark;

import cn.tomandersen.timeseries.compression.DatasetReader;

import java.nio.ByteBuffer;

/**
 * <h3>DatasetReaderDemo</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 */
public class DatasetReaderDemo {
    public static void main(String[] args) {
        // Read file and get buffer.
        String filePath = "C:\\Users\\DELL\\Desktop\\testDataset";
        DatasetReader.readAndDivideLong(filePath);
        ByteBuffer timestampBuffer = DatasetReader.getTimestampBuffer();
        ByteBuffer valueBuffer = DatasetReader.getValueBuffer();

        // Transform write mode to read mode.
        timestampBuffer.flip();
        valueBuffer.flip();

        // Print the content in the buffer.
        while (timestampBuffer.hasRemaining()) {
            System.out.println(timestampBuffer.getLong());
        }
        while (valueBuffer.hasRemaining()) {
            System.out.println(valueBuffer.getLong());
        }
    }
}
