package cn.tomandersen.timeseries.compression.universal;

import cn.tomandersen.timeseries.compression.APE.APETimestampCompressor1;
import cn.tomandersen.timeseries.compression.BitBufferWriter;
import cn.tomandersen.timeseries.compression.bitpack.BitPackValueCompressor;
import cn.tomandersen.timeseries.compression.DatasetReader;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * <h3>Universal Demo</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/18
 * @see UniversalTSCompressor
 * @see UniversalTSDecompressor
 */
public class UniversalCompressionDemo {

    public static void compressionDemo(String filename) {

        // Read the dataset corresponding to the filename.
        // Parse every row into two long type value.
        DatasetReader.readAndDivideLong(filename);
        // Get the timestamp and metric value buffer corresponding to the dataset.
        ByteBuffer uncompressedTimestampBuffer = DatasetReader.getTimestampBuffer();
        ByteBuffer uncompressedValueBuffer = DatasetReader.getValueBuffer();
//        // Switch 'Read' mode to 'Write' mode(i.e. change the limit, position, and mark).
//        uncompressedTimestampBuffer.flip();
//        uncompressedValueBuffer.flip();

        // Compress
        BitBufferWriter compressedTimestamps = new BitBufferWriter();
        BitBufferWriter compressedValues = new BitBufferWriter();
        UniversalTSCompressor tsCompressor = new UniversalTSCompressor(
                new APETimestampCompressor1(compressedTimestamps),
                new BitPackValueCompressor(compressedValues)
        );
        // Start time
        long clock = Instant.now().toEpochMilli();

        //**********************
//        tsCompressor.compress(filename,false);
        //**********************

        tsCompressor.compress(uncompressedTimestampBuffer, uncompressedValueBuffer);
        tsCompressor.close();

        // End time
        clock = Instant.now().toEpochMilli() - clock;

        // Print compressed data
        ByteBuffer compressedTimestampByteBuffer = compressedTimestamps.getBuffer();
        ByteBuffer compressedValueByteBuffer = compressedValues.getBuffer();

        compressedTimestampByteBuffer.flip();
        compressedValueByteBuffer.flip();
//        printCompressedData(compressedTimestampByteBuffer, compressedValueByteBuffer);

/*        // Decompress
        compressedTimestampByteBuffer.rewind();
        compressedValueByteBuffer.rewind();

        BitBufferReader compressedTimestampsBitInput = new BitBufferReader(compressedTimestampByteBuffer);
        BitBufferReader compressedValueBitInput = new BitBufferReader(compressedValueByteBuffer);
        ByteBuffer decompressedTimestampsBuffer = ByteBuffer.allocate(uncompressedTimestampBuffer.capacity());
        ByteBuffer decompressedValuesBuffer = ByteBuffer.allocate(uncompressedValueBuffer.capacity());
        UniversalTSDecompressor tsDecompressor = new UniversalTSDecompressor(
                new APETimestampDecompressor1(compressedTimestampsBitInput),
                new BitPackValueDecompressor(compressedValueBitInput),
                decompressedTimestampsBuffer, decompressedValuesBuffer
        );
        tsDecompressor.decompress();
        // Print decompressed data
        printDecompressedDataLong(decompressedTimestampsBuffer, decompressedValuesBuffer);*/


        printResult(
                uncompressedTimestampBuffer,
                uncompressedValueBuffer,
                compressedTimestampByteBuffer,
                compressedValueByteBuffer,
                clock
        );
    }

    private static void printDecompressedData(
            ByteBuffer decompressedTimestampsBuffer, ByteBuffer decompressedValuesBuffer
    ) {
        while (decompressedTimestampsBuffer.hasRemaining()) {
            System.out.print(decompressedTimestampsBuffer.getLong());
            System.out.print(" ");
            System.out.print(decompressedValuesBuffer.getDouble());
//            System.out.print(decompressedValuesBuffer.nextLong());
            System.out.println();
        }
    }

    private static void printDecompressedDataLong(
            ByteBuffer decompressedTimestampsBuffer, ByteBuffer decompressedValuesBuffer
    ) {
        while (decompressedTimestampsBuffer.hasRemaining()) {
            System.out.print(decompressedTimestampsBuffer.getLong());
            System.out.print(" ");
            System.out.print(decompressedValuesBuffer.getLong());
            System.out.println();
        }
    }


    private static void printCompressedData(
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
        String path = "C:\\Users\\DELL\\Desktop\\TSDataset\\with timestamps\\with abnormal timestamp\\ATimeSeriesDataset-master\\";
//        String dataset = "tmp\\Server43";
//        String dataset = "tmp\\testDataset";
//        String dataset = "UCR\\CinC_ECG_torso";
//        String dataset = "UCR\\UWaveGestureLibraryAll";
        String[] integerValueDatasets = new String[]{
                "tmp\\Server35", "tmp\\Server43", "tmp\\Server47", "tmp\\Server48",
                "tmp\\Server62", "tmp\\Server77", "tmp\\Server82", "tmp\\Server97",
                "tmp\\Server106", "tmp\\Server115"
        };
        for (String dataset : integerValueDatasets) {
            System.out.println("----------");
            System.out.println(dataset);
            compressionDemo(path + dataset);
        }
    }
}
