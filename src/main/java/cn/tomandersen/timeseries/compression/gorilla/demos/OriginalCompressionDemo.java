package cn.tomandersen.timeseries.compression.gorilla.demos;

import cn.tomandersen.timeseries.compression.DatasetReader;
import cn.tomandersen.timeseries.compression.TimeSeriesCompressor;
import cn.tomandersen.timeseries.compression.gorilla.GorillaTSCompressor;
import cn.tomandersen.timeseries.compression.gorilla.OriginalTSCompressor;
import fi.iki.yak.ts.compression.gorilla.*;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * <h3>用于测试Gorilla Compressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/11/26
 */
public class OriginalCompressionDemo {
    public static void compressAndDecompressDemo(String dataset) {
        String path = "C:\\Users\\DELL\\Desktop\\TSDataset\\with timestamps\\with abnormal timestamp\\ATimeSeriesDataset-master\\";
        String filename = path + dataset;

        DatasetReader.readAndDivideDouble(filename);
//        reader.readAndDivideLong();

        ByteBuffer uncompressedTimestampBuffer = DatasetReader.getTimestampBuffer();
        ByteBuffer uncompressedValueBuffer = DatasetReader.getValueBuffer();

//        uncompressedTimestampBuffer.flip();
//        uncompressedValueBuffer.flip();

        long now = 1523075453;
        ByteBufferBitOutput timestampOutput = new ByteBufferBitOutput();
        ByteBufferBitOutput valueOutput = new ByteBufferBitOutput();

//        // New version compression method
//        ByteBufferBitOutput output = new ByteBufferBitOutput();
//        GorillaCompressor compressor = new GorillaCompressor(now, output);
//        while (uncompressedTimestampBuffer.remaining() >= Long.BYTES) {
//            compressor.addValue(uncompressedTimestampBuffer.getLong(), uncompressedValueBuffer.getLong());
//        }
//        compressor.close();
//        System.out.println("New version");

//        // Old version compression method
//        BitOutput output = new ByteBufferBitOutput();
//        Compressor tsCompressor = new Compressor(now, output);
//        while (uncompressedTimestamps.remaining() >= Long.BYTES) {
//            tsCompressor.addValue(uncompressedTimestamps.getLong(), uncompressedValues.getLong());
//        }
//        tsCompressor.close();
//        System.out.println("Old version");

//        // Print result
//        System.out.println(dataset);
//        int uncompressedDataSize = uncompressedTimestampBuffer.capacity() + uncompressedValueBuffer.capacity();
//        int compressedDataSize = output.getByteBuffer().position();
//        System.out.println(uncompressedDataSize + "B" + " -> " + compressedDataSize + "B");
//        System.out.println("Compression ratio: " + (float) uncompressedDataSize / compressedDataSize);

        // Compress
        long clock = Instant.now().toEpochMilli(); // Compression start moment
        // Reconstruct version compression
        OriginalTSCompressor originalTSCompressor =
                new OriginalTSCompressor(timestampOutput, valueOutput, now);
        while (uncompressedTimestampBuffer.remaining() >= Long.BYTES) {
            originalTSCompressor.addValue(uncompressedTimestampBuffer.getLong(), uncompressedValueBuffer.getLong());
        }
        originalTSCompressor.close();

        clock = Instant.now().toEpochMilli() - clock; // Compression end moment

        // Print result
        System.out.println(dataset);
        ByteBuffer compressedTimestampBuffer = timestampOutput.getByteBuffer();
        ByteBuffer compressedValueBuffer = valueOutput.getByteBuffer();
        compressedTimestampBuffer.flip();
        compressedValueBuffer.flip();
        printResult(
                uncompressedTimestampBuffer, uncompressedValueBuffer,
                compressedTimestampBuffer, compressedValueBuffer, clock
        );
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
    }

    public static void main(String[] args) {
        String[] timestampDataset = new String[]{
                "IoT\\IoT0", "IoT\\IoT1", "IoT\\IoT2", "IoT\\IoT3", "IoT\\IoT4", "IoT\\IoT5", "IoT\\IoT6", "IoT\\IoT7"
        };
        String[] metricValueDatasetA = new String[]{
                "IoT\\IoT1", "IoT\\IoT2", "IoT\\IoT5", "IoT\\IoT7",
                "Server\\Server30", "Server\\Server32", "Server\\Server35", "Server\\Server43",
                "Server\\Server47", "Server\\Server48",
                "UCR\\Haptics", "UCR\\UWaveGestureLibraryAll", "UCR\\HandOutlines", "UCR\\StarLightCurves"
        };

        String[] metricValueDatasetB = new String[]{
                "Server\\Server57", "Server\\Server62", "Server\\Server66", "Server\\Server77",
                "Server\\Server82", "Server\\Server94", "Server\\Server97", "Server\\Server106",
                "Server\\Server109", "Server\\Server115",
                "UCR\\Phoneme", "UCR\\InlineSkate", "UCR\\MALLAT", "UCR\\CinC_ECG_torso"
        };

        String[] UCRDataset = new String[]{
                "UCR\\CinC_ECG_torso", "UCR\\InlineSkate", "UCR\\MALLAT", "UCR\\Phoneme",
                "UCR\\Haptics", "UCR\\UWaveGestureLibraryAll", "UCR\\HandOutlines",
                "UCR\\StarLightCurves"
        };

        for (String dataset : metricValueDatasetA) {
            System.out.println("---------");
            compressAndDecompressDemo(dataset);
        }
//        compressAndDecompressDemo("UCR\\HandOutlines");

    }


}
