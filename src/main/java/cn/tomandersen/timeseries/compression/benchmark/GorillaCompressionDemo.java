package cn.tomandersen.timeseries.compression.benchmark;

import cn.tomandersen.timeseries.compression.BitBufferReader;
import cn.tomandersen.timeseries.compression.BitBufferWriter;
import cn.tomandersen.timeseries.compression.DatasetReader;
import cn.tomandersen.timeseries.compression.gorilla.*;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * <h3>GorillaCompressionDemo</h3>
 * Gorilla compression and decompression demo.
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/2
 * @see GorillaTimestampCompressor
 * @see GorillaValueCompressor
 */
public class GorillaCompressionDemo extends CompressionDemo {
    // Set for statistic
    public static int a0, a1, a2, a3, a4;
    public static int b0, b1, b2;
    public static int c0, c1, c2;
    public static int d0, d1, d2;

    public static void compressionDemo(String filename, boolean isLongOrDoubleValue) {

        // Read file and get buffer.
//        String filePath = "C:\\Users\\DELL\\Desktop\\testDataset";
//        String filePath = "C:\\Users\\DELL\\Desktop\\TSDataset\\with timestamps\\with abnormal timestamp\\ATimeSeriesDataset-master\\IoT\\IoT2";
        if (!isLongOrDoubleValue) {
            DatasetReader.readAndDivideDouble(filename); // Parse metric value as double type.
        }
        else {
            DatasetReader.readAndDivideLong(filename); // Parse metric value as long type.
        }

        ByteBuffer uncompressedTimestampBuffer = DatasetReader.getTimestampBuffer();
        ByteBuffer uncompressedValueBuffer = DatasetReader.getValueBuffer();
        // Switch write mode to read mode.
        uncompressedTimestampBuffer.flip();
        uncompressedValueBuffer.flip();

        // Compress
        BitBufferWriter compressedTimestampOutput = new BitBufferWriter();
        BitBufferWriter compressedValueBitOutput = new BitBufferWriter();
        GorillaTSCompressor tsCompressor =
                new GorillaTSCompressor(compressedTimestampOutput, compressedValueBitOutput);

        // Start time
        long clock = Instant.now().toEpochMilli();

        tsCompressor.compress(uncompressedTimestampBuffer, uncompressedValueBuffer);
        tsCompressor.close();

        // End time
        clock = Instant.now().toEpochMilli() - clock;


        // Print compressed data

        ByteBuffer compressedTimestampByteBuffer = compressedTimestampOutput.getBuffer();
        ByteBuffer compressedValueByteBuffer = compressedValueBitOutput.getBuffer();

        compressedTimestampByteBuffer.flip();
        compressedValueByteBuffer.flip();

        // Print the compressed data
        printCompressedData(compressedTimestampByteBuffer);
        printCompressedData(compressedValueByteBuffer);

//        printCompressedData(compressedTimestampByteBuffer, compressedValueByteBuffer);
        printResult(
                uncompressedTimestampBuffer,
                uncompressedValueBuffer,
                compressedTimestampByteBuffer,
                compressedValueByteBuffer,
                clock);

        // Decompress
        compressedTimestampByteBuffer.flip();
        compressedValueByteBuffer.flip();

        ByteBuffer decompressedTimestampsByteBuffer = ByteBuffer.allocate(uncompressedTimestampBuffer.capacity());
        ByteBuffer decompressedValuesByteBuffer = ByteBuffer.allocate(uncompressedValueBuffer.capacity());
        GorillaTSDecompressor tsDecompressor = new GorillaTSDecompressor(
                new GorillaTimestampDecompressor(new BitBufferReader(compressedTimestampByteBuffer)),
                new GorillaValueDecompressor(new BitBufferReader(compressedValueByteBuffer)),
                decompressedTimestampsByteBuffer,
                decompressedValuesByteBuffer
        );
        tsDecompressor.decompress();

        // Print decompressed data
        printDecompressedData(
                decompressedTimestampsByteBuffer,
                decompressedValuesByteBuffer,
                isLongOrDoubleValue
        );


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
                "Timestamps: " + uncompressedTimestampSize + "B" + " -> " + compressedTimestampSize + "B"
        );
        System.out.println(
                "Timestamps compression ratio: " + (float) uncompressedTimestampSize / compressedTimestampSize
        );

        System.out.println(
                "Metric values: " + uncompressedValueSize + "B" + " -> " + compressedValueSize + "B"
        );

        System.out.println(
                "Metric values compression ratio: " + (float) uncompressedValueSize / compressedValueSize
        );

        float ratio = (float) (uncompressedTimestampSize + uncompressedValueSize)
                / (compressedTimestampSize + compressedValueSize);
        System.out.println("Compression ratio: " + ratio);
        System.out.println("Compression time: " + compressionTime);

        /*// Print the distribution of bucket.
        System.out.println("Timestamps distribution: " + a0 + " " + a1 + " " + a2 + " " + a3 + " " + a4);
        System.out.println("Metric value distribution: " + b0 + " " + b1 + " " + b2);
        System.out.println("XOR Value leading zeros distribution: " + c0 + " " + c1 + " " + c2);
        System.out.println("XOR Value trailing zeros distribution: " + d0 + " " + d1 + " " + d2);*/
    }

    public static void main(String[] args) {
        String path = "C:\\Users\\DELL\\Desktop\\TSDataset\\with timestamps\\with abnormal timestamp\\ATimeSeriesDataset-master\\";
//        String dataset = "tmp\\Server35";
        String dataset = "tmp\\testDataset";
//        String dataset = "UCR\\CinC_ECG_torso";
//        String dataset = "UCR\\UWaveGestureLibraryAll";

        compressionDemo(path + dataset, true);
//        if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN)
//            System.out.println("This is big-endian storage mode.");
//        else System.out.println("This is little-endian storage mode.");
    }
}
