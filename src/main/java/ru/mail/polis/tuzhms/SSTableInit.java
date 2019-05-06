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

    private File data;
    private File ssTableFile;
    private File blob;
    private final NavigableMap<ByteBuffer, SSTableRecord> ssTable;
    int position;

    SSTableInit(File data) {
        if (!data.exists()) {
            if (data.mkdir()) {
                log.info("Папка успешно создалась");
            } else {
                String message = "Ошибка при создании папки";
                log.error(message);
                throw new RuntimeException(message);
            }
        }
        this.data = data;
        ssTable = new TreeMap<>();
        String[] files = data.list();
        List list = Arrays.asList(files);
        if (list.contains(SSTABLE_FILE) && list.contains(BLOB_FILE)) {
            try{
                openSSTable(data);
            } catch (IOException e) {
                String message = "Ошибка при открытии файлов";
                log.error(message, e);
                throw new RuntimeException(message, e);
            }
        } else {
            try{
                createSSTable(data);
            } catch (IOException e) {
                String message = "Ошибка при создании файлов";
                log.error(message, e);
                throw new RuntimeException(message, e);
            }
        }
    }

    public void openSSTable(File data) throws IOException {
        ssTableFile = new File(data, SSTABLE_FILE);
        blob = new File(data, BLOB_FILE);
        Files.lines(ssTableFile.toPath(), Charsets.UTF_8)
                .map(SSTableRecord::parseString)
                .forEach(record -> ssTable.put(record.getKey(), record));
    }

    public void createSSTable(File data) throws IOException {
        ssTableFile = new File(data, SSTABLE_FILE);
        if(ssTableFile.createNewFile()) {
            log.info("Файл " + SSTABLE_FILE + " создан");
        } else {
            String message = "Ошибка при создании файла " + SSTABLE_FILE;
            log.error(message);
            throw new IOException(message);
        }
        blob = new File(data, BLOB_FILE);
        if(blob.createNewFile()) {
            log.info("Файл " + BLOB_FILE + " создан");
        } else {
            String message = "Ошибка при создании файла " + BLOB_FILE;
            log.error(message);
            throw new IOException(message);
        }
    }

    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws NoSuchElementException, IOException {
        if (ssTable.containsKey(key)) {
            SSTableRecord record = ssTable.get(key);
            byte[] value = new byte[record.getLen()];
            InputStream inputStream = Files.newInputStream(blob.toPath(), StandardOpenOption.READ);
            try {
                inputStream.skip(record.getOff());
                inputStream.read(value, 0, record.getLen());
            } catch (Exception e) {
                log.error("Завалилось чтение в блобе", e);
                throw new RuntimeException("Завалилось чтение в блобе", e);
            }
            return ByteBuffer.wrap(value);
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void updateSSTable(@NotNull NavigableMap<ByteBuffer, Record> memTable) throws IOException {
        mergeTables(memTable);
        File oldBlob = new File(data, "_" + BLOB_FILE);
        renameFile(blob, oldBlob);
        deleteFile(ssTableFile);
        createSSTable(data);
        position = 0;
        ssTable.forEach((key, value) -> {
            if (value == null) {
                ByteBuffer byteBuffer = memTable.get(key).getValue();
                byte[] bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
                SSTableRecord ssTableRecord = new SSTableRecord(key, position, bytes.length);
                position += bytes.length;
                try {
                    Files.write(blob.toPath(), bytes, StandardOpenOption.APPEND);
                    Files.write(ssTableFile.toPath(), ssTableRecord.getRecordString().getBytes(Charsets.UTF_8),
                            StandardOpenOption.APPEND);
                    ssTable.put(key, ssTableRecord);
                } catch (IOException e) {
                    String message = "Ошибка записи в блоб";
                    log.error(message, e);
                    throw new RuntimeException(message, e);
                }
            } else {
                byte[] buffer = new byte[value.getLen()];
                try {
                    InputStream blobInputStream = Files.newInputStream(oldBlob.toPath(), StandardOpenOption.READ);
                    blobInputStream.skip(value.getOff());
                    blobInputStream.read(buffer, 0, value.getLen());
                    blobInputStream.close();
                    Files.write(blob.toPath(), buffer, StandardOpenOption.APPEND);
                    SSTableRecord ssTableRecord = new SSTableRecord(key, position, buffer.length);
                    Files.write(ssTableFile.toPath(), ssTableRecord.getRecordString().getBytes(Charsets.UTF_8),
                            StandardOpenOption.APPEND);
                    position += buffer.length;
                    ssTable.put(key, ssTableRecord);
                } catch (IOException e) {
                    String message = "Ошибка записи в блоб";
                    log.error(message, e);
                    throw new RuntimeException(message, e);
                }
            }
        });
        deleteFile(oldBlob);
    }

    private void mergeTables(@NotNull NavigableMap<ByteBuffer, Record> memTable) {
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

    private void renameFile(File oldFile, File newFile) throws IOException {
        if (oldFile.renameTo(newFile)) {
            log.info("Файл " + oldFile + " переивенован в " + newFile);
        } else {
            String message = "Ошибка при переименовании файла " + oldFile;
            log.error(message);
            throw new IOException(message);
        }
    }

    private void deleteFile(File file) throws IOException {
        if (file.delete()) {
            log.info("Файл " + file + " удалён");
        } else {
            String message = "Ошибка при удалении файла " + file;
            log.error(message);
            throw new IOException(message);
        }
    }

    @Override
    public Iterator<SSTableRecord> iterator(@NotNull ByteBuffer from) throws IOException {
        return ssTable.tailMap(from).values().iterator();
    }
}
