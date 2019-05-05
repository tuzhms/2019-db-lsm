package ru.mail.polis.tuzhms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.Record;

public interface SSTableApi {
    ByteBuffer get(@NotNull ByteBuffer key) throws NoSuchElementException, IOException;

    void updateSSTable(@NotNull NavigableMap<ByteBuffer, Record> memTable) throws IOException;
}
