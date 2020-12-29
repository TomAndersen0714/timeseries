package cn.tomandersen.timeseries.compression.APE.demos;

import cn.tomandersen.timeseries.compression.APE.APETSCompressor;
import cn.tomandersen.timeseries.compression.DatasetReader;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * <h3>APE Compression Demo</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 */
public class APECompressionDemo {
    // Set for statistic
    public static int a0, a1, a2, a3, a4; // Timestamp distribution
    public static int b0, b1, b2; // Metric value distribution
    public static int c0, c1, c2; // XOR value leading zeros distribution
    public static int d0, d1, d2; // XOR value trailing zeros distribution

    public static void compressionDemo(String filename) {

        // Read file and get buffer.
//        String filePath = "C:\\Users\\DELL\\Desktop\\testDataset";
//        String filename = "C:\\Users\\DELL\\Desktop\\TSDataset\\with timestamps\\with abnormal timestamp\\ATimeSeriesDataset-master\\IoT\\IoT2";
        // Parse every row into a long type and a double type value.
        DatasetReader.readAndDivideDouble(filename);
        // Get the timestamp and metric value buffer corresponding to the dataset.
        ByteBuffer uncompressedTimestampBuffer = DatasetReader.getTimestampBuffer();
        ByteBuffer uncompressedValueBuffer = DatasetReader.getValueBuffer();

//        // Switch write mode to read mode.
//        uncompressedTimestampBuffer.flip();
//        uncompressedValueBuffer.flip();

        // Compress
        ByteBufferBitOutput compressedTimestampOutput = new ByteBufferBitOutput();
        ByteBufferBitOutput compressedValueOutput = new ByteBufferBitOutput();
        APETSCompressor tsCompressor =
                new APETSCompressor(compressedTimestampOutput, compressedValueOutput);
//        UniversalTSCompressor tsCompressor = new UniversalTSCompressor(
//                new APETimestampCompressor1(compressedTimestampOutput),
//                new APEValueCompressor2(compressedValueOutput)
//        );

        // Start time
        long clock = Instant.now().toEpochMilli();

        tsCompressor.compress(uncompressedTimestampBuffer, uncompressedValueBuffer);
        tsCompressor.close();

        // End time
        clock = Instant.now().toEpochMilli() - clock;

        // Print compressed data
        ByteBuffer compressedTimestampByteBuffer = compressedTimestampOutput.getByteBuffer();
        ByteBuffer compressedValueByteBuffer = compressedValueOutput.getByteBuffer();

        compressedTimestampByteBuffer.flip();
        compressedValueByteBuffer.flip();

//        printCompressedData(compressedTimestampByteBuffer, compressedValueByteBuffer);
        printResult(
                uncompressedTimestampBuffer,
                uncompressedValueBuffer,
                compressedTimestampByteBuffer,
                compressedValueByteBuffer,
                clock
        );
/*
        // Decompress
        compressedTimestampByteBuffer.rewind();
        compressedValueByteBuffer.rewind();

        ByteBufferBitInput compressedTimestampsBitInput = new ByteBufferBitInput(compressedTimestampByteBuffer);
        ByteBufferBitInput compressedValuesBitInput = new ByteBufferBitInput((compressedValueByteBuffer));
        ByteBuffer decompressedTimestampsByteBuffer = ByteBuffer.allocate(uncompressedTimestampBuffer.capacity());
        ByteBuffer decompressedValuesByteBuffer = ByteBuffer.allocate(uncompressedValueBuffer.capacity());
        APETSDecompressor tsDecompressor = new APETSDecompressor(
                compressedTimestampsBitInput,
                compressedValuesBitInput,
                decompressedTimestampsByteBuffer,
                decompressedValuesByteBuffer
        );
        tsDecompressor.decompress();*/

        // Print decompressed data
//        printDecompressedData(decompressedTimestampsByteBuffer, decompressedValuesByteBuffer);
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

/*        // Print the distribution of bucket.
        System.out.println("Timestamps distribution: " +
                a0 + " " + a1 + " " + a2 + " " + a3 + " " + a4);
        System.out.println("Metric value distribution: " +
                b0 + " " + b1 + " " + b2);
        System.out.println("XOR Value leading zeros distribution: " +
                c0 + " " + c1 + " " + c2);
        System.out.println("XOR Value trailing zeros distribution: " +
                d0 + " " + d1 + " " + d2);*/
    }

    private static void printDecompressedData(
            ByteBuffer decompressedTimestampsByteBuffer, ByteBuffer decompressedValuesByteBuffer
    ) {
        while (decompressedTimestampsByteBuffer.hasRemaining()) {
            System.out.print(decompressedTimestampsByteBuffer.getLong());
            System.out.print(" ");
            System.out.print(decompressedValuesByteBuffer.getDouble());
            System.out.println();
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
        String filename = "C:\\Users\\DELL\\Desktop\\TSDataset\\with timestamps\\with abnormal timestamp\\ATimeSeriesDataset-master\\IoT\\IoT2";
        compressionDemo(filename);
    }
}
