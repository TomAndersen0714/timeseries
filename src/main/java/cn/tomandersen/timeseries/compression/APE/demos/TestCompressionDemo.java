package cn.tomandersen.timeseries.compression.APE.demos;

import java.nio.ByteBuffer;

/**
 * <h3>Test Compression Demo</h3>
 * Used for test every combination of timestamp compressor and value compressor.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/17
 */
public class TestCompressionDemo {
    // Set for statistic
    public static int a0, a1, a2, a3, a4; // Timestamp distribution
    public static int b0, b1, b2; // Metric value distribution
    public static int c0, c1, c2; // XOR value leading zeros distribution
    public static int d0, d1, d2; // XOR value trailing zeros distribution

    public static void compressionDemo() {

    }

    private static void printResult(
            ByteBuffer uncompressedTimestampBuffer,
            ByteBuffer uncompressedValueBuffer,
            ByteBuffer compressedTimestampBuffer,
            ByteBuffer compressedValueBuffer,
            long compressionTime
    ) {
        int uncompressedTimestampSize = uncompressedTimestampBuffer.limit();
        int uncompressedValueSize = uncompressedValueBuffer.limit();
        int compressedTimestampSize = compressedTimestampBuffer.limit();
        int compressedValueSize = compressedValueBuffer.limit();
        System.out.println(
                "Timestamps: " +
                        uncompressedTimestampSize + "B" + " -> " +
                        compressedTimestampSize + "B"
        );
        System.out.println("Timestamps compression ratio: " + (float) uncompressedTimestampSize / compressedTimestampSize);

        System.out.println(
                "Metric values: " +
                        uncompressedValueSize + "B" + " -> " +
                        compressedValueSize + "B"
        );

        System.out.println("Metric values compression ratio: " + (float) uncompressedValueSize / compressedValueSize);

        float ratio = (float) (uncompressedTimestampSize + uncompressedValueSize)
                / (compressedTimestampSize + compressedValueSize);
        System.out.println("Compression ratio: " + ratio);
        System.out.println("Compression time: " + compressionTime);

        // Print the distribution of bucket.
        System.out.println("Timestamps distribution: " +
                a0 + " " + a1 + " " + a2 + " " + a3 + " " + a4);
        System.out.println("Metric value distribution: " +
                b0 + " " + b1 + " " + b2);
        System.out.println("XOR Value leading zeros distribution: " +
                c0 + " " + c1 + " " + c2);
        System.out.println("XOR Value trailing zeros distribution: " +
                d0 + " " + d1 + " " + d2);
    }

    private static void printDecompressedData(
            ByteBuffer decompressedTimestampsByteBuffer, ByteBuffer decompressedValuesByteBuffer
    ) {
        while (decompressedTimestampsByteBuffer.hasRemaining()) {
            System.out.println(decompressedTimestampsByteBuffer.getLong() + " " + decompressedValuesByteBuffer.getDouble());
        }
    }


    private static void printCompressedData(
            ByteBuffer compressedTimestampByteBuffer, ByteBuffer compressedValueByteBuffer
    ) {
        while (compressedTimestampByteBuffer.hasRemaining()) {
            System.out.printf("%02X ", Byte.toUnsignedInt(compressedTimestampByteBuffer.get()));
        }
        System.out.println();
        while (compressedValueByteBuffer.hasRemaining()) {
            System.out.printf("%02X ", Byte.toUnsignedInt(compressedValueByteBuffer.get()));
        }
        System.out.println();
    }


    public static void main(String[] args) {

    }
}
