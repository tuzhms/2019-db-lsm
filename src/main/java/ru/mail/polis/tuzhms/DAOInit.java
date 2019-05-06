package ru.mail.polis.tuzhms;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import static ru.mail.polis.tuzhms.Constants.DELETED_RECORD;
import static ru.mail.polis.tuzhms.Constants.MIN_FREE_MEMORY;

public class DAOInit implements DAO {
    private static final Logger log = LoggerFactory.getLogger(DAOInit.class);

    private final NavigableMap<ByteBuffer, Record> memTable;
    private final SSTableApi ssTable;

    public DAOInit(final File data) {
        memTable = new TreeMap<>();
        ssTable = new SSTableInit(data);
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final Iterator<Record> memIterator = memTable.tailMap(from).values().iterator();
        final Iterator<SSTableRecord> ssIterator = ssTable.iterator(from);
        final Iterator<Record> ssRecordIterator = Iterators.transform(ssIterator,
                ssTableRecord -> {
                    try {
                        return Record.of(ssTableRecord.getKey(), ssTable.get(ssTableRecord.getKey()));
                    } catch (IOException e) {
                        log.error("Ошибка при чтении из блоба", e);
                        return Record.of(ssTableRecord.getKey(), ByteBuffer.wrap(new byte[0]));
                    }
                });
        final List<Iterator<Record>> iteratorList = new ArrayList<>();
        iteratorList.add(Iterators.filter(memIterator, record -> record != DELETED_RECORD));
        iteratorList.add(ssRecordIterator);
        return Iterators.mergeSorted(iteratorList, Record::compareTo);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        if (memTable.containsKey(key)) {
            final Record record = memTable.get(key);
            if (record == DELETED_RECORD) {
                throw new NoSuchElementException();
            } else {
                return record.getValue();
            }
        } else {
            return ssTable.get(key);
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.put(key, Record.of(key, value));
        if (checkFreeMem()) {
            clearMemTable();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.put(key, DELETED_RECORD);
        if (checkFreeMem()) {
            clearMemTable();
        }
    }

    private boolean checkFreeMem() {
        final long freeMemory = Runtime.getRuntime().freeMemory();
        final boolean isEndingMemory = freeMemory < MIN_FREE_MEMORY;
        if (isEndingMemory) {
            log.info("Заканчивается свободная память: " + freeMemory / (1024 * 1024) + " МБ");
        }
        return isEndingMemory;
    }

    private void clearMemTable() throws IOException {
        try {
            log.info("Начало записи данных в SSTable");
            ssTable.updateSSTable(memTable);
            log.info("Запись данных в SSTable завершена");
            memTable.clear();
        } catch (IOException e) {
            log.error("Ошибка записи SSTable", e);
            throw new IOException("Ошибка записи SSTable", e);
        }
    }

    @Override
    public void close() throws IOException {
        clearMemTable();
    }
}
