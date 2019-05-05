package ru.mail.polis.tuzhms;

import java.nio.ByteBuffer;

import ru.mail.polis.Record;

public class Constants {
    public static final Record DELETED_RECORD = Record.of(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]));

    public static final long MAX_MEMORY = 128 * 1024 * 1024; // 128M
    public static final long MIN_FREE_MEMORY = 32 * 1024 * 1024; // 16M

    public static final String SSTABLE_FILE = "sstable.db";
    public static final String BLOB_FILE = "blob.db";
}
