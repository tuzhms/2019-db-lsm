package ru.mail.polis.tuzhms;

import java.nio.ByteBuffer;

import ru.mail.polis.Record;

import static ru.mail.polis.DAOFactory.MAX_HEAP;

public final class Constants {

    public static final Record DELETED_RECORD = Record.of(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]));

    public static final long MIN_FREE_MEMORY = MAX_HEAP/8; // 16M

    public static final String SSTABLE_FILE = "sstable.db";
    public static final String BLOB_FILE = "blob.db";

    private Constants() {}
}
