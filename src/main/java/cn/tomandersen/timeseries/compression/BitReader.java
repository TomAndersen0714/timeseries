package cn.tomandersen.timeseries.compression;

import java.nio.ByteBuffer;

public interface BitReader {

    /**
     * Read the next bit and returns true if is '1' bit and false if not.
     *
     * @return true(i.e. ' 1 ' bit), false(i.e. '0' bit)
     */
    boolean nextBit();

    /**
     * Read bit continuously, until next '0' bit is found or the number of
     * read bits reach the value of 'maxBits'.
     *
     * @param maxBits How many bits at maximum until returning
     * @return Integer value of the read bits
     */
    int nextControlBits(int maxBits);

    /**
     * Read next 8 bits and returns the byte value.
     *
     * @return byte type value
     */
    byte nextByte();

    /**
     * Read next n(n<=8) bits and return the corresponding byte type value.
     *
     * @param bits the number of bit need to read
     * @return byte type value
     */
    byte nextByte(int bits);


    /**
     * Read next 32 bits and return the corresponding integer type value.
     *
     * @return integer type value.
     */
    int nextInt();

    /**
     * Read next n(n<=32) bits and return the corresponding integer type value.
     *
     * @param bits the number of bit to read.
     * @return integer type value.
     */
    int nextInt(int bits);

    /**
     * Read next 64 bits and return the corresponding long type value.
     *
     * @return long type value .
     */
    long nextLong();

    /**
     * Read next n(n<=64) bits and return the corresponding long type value.
     *
     * @param bits the number of bit to read.
     * @return long type value.
     */
    long nextLong(int bits);

    /**
     * Read next 32 bits and return the corresponding float type value.
     *
     * @return float type value.
     */
    float nextFloat();

    /**
     * Read next n(n<=32) bits and return the corresponding float type value.
     *
     * @param bits the number of bit to read
     * @return float type value.
     */
    float nextFloat(int bits);

    /**
     * Read next 64 bits and return the corresponding double type value.
     *
     * @return double type value.
     */
    double nextDouble();

    /**
     * Read next n(n<=64) bits and return the corresponding double type value.
     *
     * @param bits the number of bit to read.
     * @return double type value.
     */
    double nextDouble(int bits);

    /**
     * Returns the output buffer.
     */
    ByteBuffer getBuffer();
}
