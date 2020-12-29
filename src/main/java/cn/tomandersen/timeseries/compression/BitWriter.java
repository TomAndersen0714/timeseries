package cn.tomandersen.timeseries.compression;

import java.nio.ByteBuffer;

public interface BitWriter {

    /**
     * Write a '1' bit into the buffer.
     */
    void writeOneBit();

    /**
     * Write a '0' bit into the buffer.
     */
    void writeZeroBit();


    /**
     * Write the specific least significant bits of value into the buffer.
     *
     * @param value value need to write
     * @param bits  the number of bits need to write
     */
    void writeBits(long value, int bits);

    /**
     * Write a single byte into the buffer.
     *
     * @param b the byte value need to write.
     */
    void writeByte(byte b);

    /**
     * Write a int type value into the buffer.
     *
     * @param value the int type value need to write.
     */
    void writeInt(int value);

    /**
     * Write a long type value into the buffer.
     *
     * @param value the long type value need to write.
     */
    void writeLong(long value);

    /**
     * Write a float type value into the buffer.
     *
     * @param value the float type value need to write.
     */
    void writeFloat(float value);

    /**
     * Write a double type value into the buffer.
     *
     * @param value the double type value need to write.
     */
    void writeDouble(double value);

    /**
     * Write the cached byte(s) to the buffer.
     */
    void flush();

    /**
     * Returns the output buffer.
     */
    ByteBuffer getBuffer();
}
