package ru.mail.polis.tuzhms;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

import ru.mail.polis.Record;

import static ru.mail.polis.tuzhms.Constants.BLOB_FILE;
import static ru.mail.polis.tuzhms.Constants.DELETED_RECORD;
import static ru.mail.polis.tuzhms.Constants.SSTABLE_FILE;

public class SSTableInit implements SSTableApi {
    private static final Logger log = LoggerFactory.getLogger(SSTableInit.class);

    private final File data;
    private File ssTableFile;
    private File blob;
    private final NavigableMap<ByteBuffer, SSTableRecord> ssTable;
    int position;

    SSTableInit(final File data) {
        if (!data.exists()) {
            if (!data.mkdir()) {
                final String message = "Ошибка при создании папки";
                log.error(message);
                throw new FileOperationException(message);
            }
        }
        this.data = data;
        ssTable = new TreeMap<>();
        final String[] files = data.list();
        final List list = Arrays.asList(files);
        if (list.contains(SSTABLE_FILE) && list.contains(BLOB_FILE)) {
            try{
                openSSTable(data);
            } catch (IOException e) {
                final String message = "Ошибка при открытии файлов";
                log.error(message, e);
                throw new FileOperationException(message, e);
            }
        } else {
            try{
                createSSTable(data);
            } catch (IOException e) {
                final String message = "Ошибка при создании файлов";
                log.error(message, e);
                throw new FileOperationException(message, e);
            }
        }
    }

    private void openSSTable(final File data) throws IOException {
        ssTableFile = new File(data, SSTABLE_FILE);
        blob = new File(data, BLOB_FILE);
        Files.lines(ssTableFile.toPath(), Charsets.UTF_8)
                .map(SSTableRecord::parseString)
                .forEach(record -> ssTable.put(record.getKey(), record));
    }


    private void createSSTable(final File data) throws IOException {
        ssTableFile = new File(data, SSTABLE_FILE);
        if(!ssTableFile.createNewFile()) {
            final String message = "Ошибка при создании файла " + SSTABLE_FILE;
            log.error(message);
            throw new IOException(message);
        }
        blob = new File(data, BLOB_FILE);
        if(!blob.createNewFile()) {
            final String message = "Ошибка при создании файла " + BLOB_FILE;
            log.error(message);
            throw new IOException(message);
        }
    }

    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws NoSuchElementException, IOException {
        if (ssTable.containsKey(key)) {
            final SSTableRecord record = ssTable.get(key);
            final byte[] value = new byte[record.getLen()];
            final InputStream inputStream = Files.newInputStream(blob.toPath(), StandardOpenOption.READ);
            try {
                if (inputStream.skip(record.getOff()) != record.getOff()) throw new IOException();
                inputStream.read(value, 0, record.getLen());
            } catch (IOException e) {
                log.error("Завалилось чтение в блобе", e);
                throw new IOException("Завалилось чтение в блобе", e);
            } finally {
                inputStream.close();
            }
            return ByteBuffer.wrap(value);
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void updateSSTable(@NotNull final NavigableMap<ByteBuffer, Record> memTable) throws IOException {
        mergeTables(memTable);
        final File oldBlob = new File(data, "_" + BLOB_FILE);
        renameFile(blob, oldBlob);
        deleteFile(ssTableFile);
        createSSTable(data);
        position = 0;
        ssTable.forEach((key, value) -> {
            if (value == null) {
                final ByteBuffer byteBuffer = memTable.get(key).getValue();
                final byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                final SSTableRecord ssTableRecord = new SSTableRecord(key, position, bytes.length);
                position += bytes.length;
                try {
                    Files.write(blob.toPath(), bytes, StandardOpenOption.APPEND);
                    Files.write(ssTableFile.toPath(), ssTableRecord.getRecordString().getBytes(Charsets.UTF_8),
                            StandardOpenOption.APPEND);
                    ssTable.put(key, ssTableRecord);
                } catch (IOException e) {
                    final String message = "Ошибка записи в блоб";
                    log.error(message, e);
                    throw new FileOperationException(message, e);
                }
            } else {
                final byte[] buffer = new byte[value.getLen()];
                try {
                    final InputStream blobInputStream = Files.newInputStream(oldBlob.toPath(), StandardOpenOption.READ);
                    if(blobInputStream.skip(value.getOff()) != value.getOff()) throw new IOException();
                    blobInputStream.read(buffer, 0, value.getLen());
                    blobInputStream.close();
                    Files.write(blob.toPath(), buffer, StandardOpenOption.APPEND);
                    final SSTableRecord ssTableRecord = new SSTableRecord(key, position, buffer.length);
                    Files.write(ssTableFile.toPath(), ssTableRecord.getRecordString().getBytes(Charsets.UTF_8),
                            StandardOpenOption.APPEND);
                    position += buffer.length;
                    ssTable.put(key, ssTableRecord);
                } catch (IOException e) {
                    final String message = "Ошибка записи в блоб";
                    log.error(message, e);
                    throw new FileOperationException(message, e);
                }
            }
        });
        deleteFile(oldBlob);
    }

    private void mergeTables(@NotNull final NavigableMap<ByteBuffer, Record> memTable) {
        memTable.forEach((key, value) -> {
            if (value == DELETED_RECORD) {
                //Удаляем запись из таблицы
                ssTable.remove(key);
            } else {
                //Изменяем или добовляем запись в таблице
                ssTable.put(key, null);
            }
        });
    }

    private void renameFile(final File oldFile, final File newFile) throws IOException {
        if (!oldFile.renameTo(newFile)) {
            final String message = "Ошибка при переименовании файла " + oldFile;
            log.error(message);
            throw new IOException(message);
        }
    }

    private void deleteFile(final File file) throws IOException {
        Files.delete(file.toPath());
    }

    @Override
    public Iterator<SSTableRecord> iterator(@NotNull final ByteBuffer from) throws IOException {
        return ssTable.tailMap(from).values().iterator();
    }
}
