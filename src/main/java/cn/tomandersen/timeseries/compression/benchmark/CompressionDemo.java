package cn.tomandersen.timeseries.compression.benchmark;

import java.nio.ByteBuffer;

/**
 * <h3>CompressionDemo</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/30
 */
public abstract class CompressionDemo {

    protected static void printCompressedData(
            ByteBuffer compressedTimestampBuffer, ByteBuffer compressedValueBuffer
    ) {
        while (compressedTimestampBuffer.hasRemaining()) {
            System.out.printf("%02X ", Byte.toUnsignedInt(compressedTimestampBuffer.get()));
        }
        System.out.println();
        while (compressedValueBuffer.hasRemaining()) {
            System.out.printf("%02X ", Byte.toUnsignedInt(compressedValueBuffer.get()));
        }
        System.out.println();
    }

    protected static void printDecompressedData(
            ByteBuffer decompressedTimestampsBuffer, ByteBuffer decompressedValuesBuffer,
            boolean isLongOrDoubleValue
    ) {
        while (decompressedTimestampsBuffer.hasRemaining()) {
            System.out.print(decompressedTimestampsBuffer.getLong());
            System.out.print(" ");
            if (!isLongOrDoubleValue)
                System.out.print(decompressedValuesBuffer.getDouble());
            else
                System.out.print(decompressedValuesBuffer.getLong());
            System.out.println();
        }
    }

    protected static void printResult(
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
    }
}
