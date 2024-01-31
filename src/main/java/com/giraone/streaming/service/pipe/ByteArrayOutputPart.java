package com.giraone.streaming.service.pipe;

/**
 * Output byte array information
 * @param array the array to read
 * @param offset the offset, where to start reading - typically 0
 * @param length the number of bytes to read
 */
@SuppressWarnings("java:S6218")
public record ByteArrayOutputPart(byte[] array, int offset, int length) {

    /**
     * Convert to string by using offset and length.
     * @return string representation
     */
    public String toString() {
        return new String(array(), offset(), length());
    }
}
