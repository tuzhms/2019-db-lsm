package ru.mail.polis.tuzhms;

public class FileOperationException extends RuntimeException {

    public FileOperationException(final String message, final Throwable e) {
        super(message, e);
    }

    public FileOperationException(final String message) {
        super(message);
    }
}