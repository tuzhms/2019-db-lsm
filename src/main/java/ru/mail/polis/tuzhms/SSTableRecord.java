package ru.mail.polis.tuzhms;

import java.nio.ByteBuffer;
import java.util.Objects;

public class SSTableRecord {
    /** Ключ. */
    private final ByteBuffer key;
    /** Начальный индекс записи. */
    private final int off;
    /** Длина записи. */
    private final int len;

    /**
     * Конструктор записи SSTable.
     * @param key   ключ.
     * @param off   начало.
     * @param len   смещение.
     */
    public SSTableRecord(final ByteBuffer key, final int off, final int len) {
        this.key = key;
        this.off = off;
        this.len = len;
    }

    /**
     * Парсинг строки записи из файла sstable.db.
     * @param recordString входная строка.
     * @return запись в удобном для работы формате.
     */
    public static SSTableRecord parseString(final String recordString) {
        int index = recordString.lastIndexOf(':');
        final int len = Integer.valueOf(recordString.substring(index + 1));
        String str = recordString.substring(0, index);
        index = str.lastIndexOf(':');
        final int off = Integer.valueOf(str.substring(index + 1));
        str = str.substring(0, index);
        index = str.lastIndexOf(':');
        int length = Integer.valueOf(str.substring(index + 1));
        str = str.substring(0, index);
        byte[] buffer = new byte[length];
        while (true) {
            index = str.lastIndexOf(':');
            if (index == -1) {
                buffer[0] = Byte.valueOf(str);
                break;
            } else {
                buffer[length - 1] = Byte.valueOf(str.substring(index + 1));
                str = str.substring(0, index);
            }
            length--;
        }
        final ByteBuffer key = ByteBuffer.wrap(buffer);
        return new SSTableRecord(key, off, len);
    }

    public String getRecordString() {
        final String keyString = getStringKey(key);
        return keyString + key.remaining() + ':' + off + ':' + len + '\n';
    }

    /**
     * Конвертация ключа из ByteBuffer в строку. Для записи в файл.
     * @param byteBuffer ключ в виде ByteBuffer.
     * @return  ключ в виде строки.
     */
    public static String getStringKey(final ByteBuffer byteBuffer) {
        final byte[] buffer = new byte[byteBuffer.remaining()];
        byteBuffer.duplicate().get(buffer);
        final StringBuilder stringBuilder = new StringBuilder();
        for (final byte b: buffer) {
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

    public int getLen() {
        return len;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SSTableRecord that = (SSTableRecord) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "SSTableRecord{"
                + "key='" + key + '\''
                + ", off=" + off
                + ", len=" + len
                + '}';
    }
}
