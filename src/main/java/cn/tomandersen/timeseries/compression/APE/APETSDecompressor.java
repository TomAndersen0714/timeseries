package cn.tomandersen.timeseries.compression.APE;

import cn.tomandersen.timeseries.compression.BitReader;
import cn.tomandersen.timeseries.compression.TimeSeriesDecompressor;

import java.nio.ByteBuffer;

/**
 * <h3>APE Time Series Decompressor</h3>
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/12/5
 * @see APETSCompressor
 */
public class APETSDecompressor extends TimeSeriesDecompressor {

    /**
     * Only support decompression by {@link #nextPair}.
     */
    public APETSDecompressor(BitReader compressedData) {
        super(new APETimestampDecompressor(compressedData), new APEValueDecompressor(compressedData));
    }

    /**
     * Only support decompression by {@link #decompress()}.
     */
    public APETSDecompressor(BitReader compressedData, ByteBuffer output) {
        super(
                new APETimestampDecompressor(compressedData),
                new APEValueDecompressor(compressedData),
                output
        );
    }

    /**
     * Only support decompression by {@link #decompress()}.
     */
    public APETSDecompressor(
            BitReader compressedData,
            ByteBuffer decompressedTimestampBuffer, ByteBuffer decompressedValueBuffer
    ) {
        super(
                new APETimestampDecompressor(compressedData),
                new APEValueDecompressor(compressedData),
                decompressedTimestampBuffer, decompressedValueBuffer
        );
    }

    public APETSDecompressor(
            BitReader compressedTimestamps, BitReader compressedValues,
            ByteBuffer decompressedTimestampBuffer, ByteBuffer decompressedValueBuffer
    ) {
        super(
                new APETimestampDecompressor(compressedTimestamps),
                new APEValueDecompressor(compressedValues),
                decompressedTimestampBuffer, decompressedValueBuffer
        );
    }

    public APETSDecompressor(
            BitReader compressedTimestamp, BitReader compressedValue,
            ByteBuffer decompressedOutputBuffer
    ) {
        super(
                new APETimestampDecompressor(compressedTimestamp),
                new APEValueDecompressor(compressedValue),
                decompressedOutputBuffer
        );
    }

}
