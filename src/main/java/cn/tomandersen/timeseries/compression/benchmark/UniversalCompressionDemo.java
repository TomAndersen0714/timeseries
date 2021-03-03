package cn.tomandersen.timeseries.compression.benchmark;

import cn.tomandersen.timeseries.compression.RLE.RLETimestampDecompressor;
import cn.tomandersen.timeseries.compression.BitBufferReader;
import cn.tomandersen.timeseries.compression.bitpack.BitPackValueCompressor;
import cn.tomandersen.timeseries.compression.bitpack.BitPackValueDecompressor;
import cn.tomandersen.timeseries.compression.RLE.RLETimestampCompressor;
import cn.tomandersen.timeseries.compression.DatasetReader;
import cn.tomandersen.timeseries.compression.MetricValueCompressor;
import cn.tomandersen.timeseries.compression.TimestampCompressor;
import cn.tomandersen.timeseries.compression.bucket.BucketValueCompressor;
import cn.tomandersen.timeseries.compression.bucket.BucketValueDecompressor;
import cn.tomandersen.timeseries.compression.universal.UniversalTSCompressor;
import cn.tomandersen.timeseries.compression.universal.UniversalTSDecompressor;

import java.nio.ByteBuffer;

/**
 * <h3>Universal Demo</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/18
 * @see UniversalTSCompressor
 * @see UniversalTSDecompressor
 */
public class UniversalCompressionDemo extends CompressionDemo {

    public static void compressionDemo(
            Class<? extends TimestampCompressor> timestampCompressorCls,
            Class<? extends MetricValueCompressor> metricValueCompressorCls,
            String filename, boolean isLongOrDoubleValue
    ) throws Exception {

//        // Read the dataset corresponding to the filename.
//        // Parse every row into two long type value.
//        DatasetReader.readAndDivideLong(filename);
//        // Get the timestamp and metric value buffer corresponding to the dataset.
//        ByteBuffer uncompressedTimestampBuffer = DatasetReader.getTimestampBuffer();
//        ByteBuffer uncompressedValueBuffer = DatasetReader.getValueBuffer();
//        // Switch 'Read' mode to 'Write' mode(i.e. change the limit, position, and mark).
//        uncompressedTimestampBuffer.flip();
//        uncompressedValueBuffer.flip();

        // Compress
//        BitBufferWriter compressedTimestamps = new BitBufferWriter();
//        BitBufferWriter compressedValues = new BitBufferWriter();
//        UniversalTSCompressor tsCompressor = new UniversalTSCompressor(
//                new RLETimestampCompressor(compressedTimestamps),
//                new BitPackValueCompressor(compressedValues)
//        );

        UniversalTSCompressor tsCompressor = new UniversalTSCompressor(
                timestampCompressorCls, metricValueCompressorCls
        );


//        tsCompressor.compress(uncompressedTimestampBuffer, uncompressedValueBuffer);
//        tsCompressor.close();

        //**********************
        // This method include data reading process.
        tsCompressor.compress(filename, isLongOrDoubleValue);
        tsCompressor.close();
        //**********************

        // Print compressed data
        ByteBuffer compressedTimestampByteBuffer = tsCompressor.getCompressedTimestampBuffer();
        ByteBuffer compressedValueByteBuffer = tsCompressor.getCompressedValueBuffer();

        compressedTimestampByteBuffer.flip();
        compressedValueByteBuffer.flip();
        printCompressedData(compressedTimestampByteBuffer, compressedValueByteBuffer);

        // Decompress
        ByteBuffer uncompressedTimestampBuffer = DatasetReader.getTimestampBuffer();
        ByteBuffer uncompressedValueBuffer = DatasetReader.getValueBuffer();

        compressedTimestampByteBuffer.rewind();
        compressedValueByteBuffer.rewind();

        ByteBuffer decompressedTimestampsBuffer = ByteBuffer.allocate(uncompressedTimestampBuffer.capacity());
        ByteBuffer decompressedValuesBuffer = ByteBuffer.allocate(uncompressedValueBuffer.capacity());
        UniversalTSDecompressor tsDecompressor = new UniversalTSDecompressor(
                new RLETimestampDecompressor(new BitBufferReader(compressedTimestampByteBuffer)),
//                new BitPackValueDecompressor(new BitBufferReader(compressedValueByteBuffer)),
                new BitPackValueDecompressor(new BitBufferReader(compressedValueByteBuffer)),
                decompressedTimestampsBuffer, decompressedValuesBuffer
        );
        tsDecompressor.decompress();
        // Print decompressed data
        printDecompressedData(decompressedTimestampsBuffer, decompressedValuesBuffer, isLongOrDoubleValue);
        // Print result.
        printResult(
                uncompressedTimestampBuffer,
                uncompressedValueBuffer,
                compressedTimestampByteBuffer,
                compressedValueByteBuffer,
                tsCompressor.getClock()
        );
    }

    public static void main(String[] args) throws Exception {
        String path = "C:\\Users\\DELL\\Desktop\\TSDataset\\with timestamps\\with abnormal timestamp\\ATimeSeriesDataset-master\\";
//        String dataset = "tmp\\Server35";
        String dataset = "tmp\\testDataset";
//        String dataset = "UCR\\CinC_ECG_torso";
//        String dataset = "UCR\\UWaveGestureLibraryAll";

        compressionDemo(
//                RLETimestampCompressor.class, BitPackValueCompressor.class,
                RLETimestampCompressor.class, BitPackValueCompressor.class,
                path + dataset, false);

//        compressionDemo(
//                RLETimestampCompressor.class, BucketValueCompressor.class,
//                path + dataset, false);

/*        String[] integerValueDatasets = new String[]{
                "tmp\\Server35", "tmp\\Server43", "tmp\\Server47", "tmp\\Server48",
                "tmp\\Server62", "tmp\\Server77", "tmp\\Server82", "tmp\\Server97",
                "tmp\\Server106", "tmp\\Server115"
        };

        for (String dataset : integerValueDatasets) {
            System.out.println("----------");
            System.out.println(dataset);
            compressionDemo(path + dataset, true);
        }*/
    }
}
