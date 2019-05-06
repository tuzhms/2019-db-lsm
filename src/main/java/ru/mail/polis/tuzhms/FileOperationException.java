package ru.mail.polis.tuzhms;

public class FileOperationException extends RuntimeException {

    public FileOperationException(String message, Throwable e) {
        super(message, e);
    }

    public FileOperationException(String message) {
        super(message);
    }
}