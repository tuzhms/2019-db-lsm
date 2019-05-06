package ru.mail.polis.tuzhms;

import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.base.Charsets;

public class SSTableRecord {
    /** Ключ */
    private final ByteBuffer key;
    /** Начальный индекс записи */
    private int off;
    /** Длина записи */
    private int len;

    public SSTableRecord(ByteBuffer key, int off, int len) {
        this.key = key;
        this.off = off;
        this.len = len;
    }

    public static SSTableRecord parseString(String recordString) {
        int i = recordString.lastIndexOf(':');
        int len = Integer.valueOf(recordString.substring(i + 1));
        String str = recordString.substring(0, i);
        i = str.lastIndexOf(':');
        int off = Integer.valueOf(str.substring(i + 1));
        str = str.substring(0, i);
        i = str.lastIndexOf(':');
        int length = Integer.valueOf(str.substring(i + 1));
        str = str.substring(0, i);
        byte[] buffer = new byte[length];
        while (true) {
            i = str.lastIndexOf(':');
            if (i == -1) {
                buffer[0] = Byte.valueOf(str);
                break;
            } else {
                buffer[length - 1] = Byte.valueOf(str.substring(i + 1));
                str = str.substring(0, i);
            }
            length--;
        }
        ByteBuffer key = ByteBuffer.wrap(buffer);
        return new SSTableRecord(key, off, len);
    }

    public String getRecordString() {
        String keyString = getStringKey(key);
        return "" + keyString + key.remaining() + ':' + off + ':' + len + '\n';
    }

    public static String getStringKey(ByteBuffer byteBuffer) {
        byte[] buffer = new byte[byteBuffer.remaining()];
        byteBuffer.duplicate().get(buffer);
        StringBuilder stringBuilder = new StringBuilder();
        for (byte b: buffer) {
            stringBuilder.append(b).append(':');
        }
        return new String(stringBuilder);
    }

    public ByteBuffer getKey() {
        return key;
    }

    public int getOff() {
        return off;
    }

    public void setOff(int off) {
        this.off = off;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SSTableRecord that = (SSTableRecord) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "SSTableRecord{" +
                "key='" + key + '\'' +
                ", off=" + off +
                ", len=" + len +
                '}';
    }
}
