package io.debezium.server.iceberg.typeconversion;

/**
 * Exception thrown when type conversion fails or would result in data loss
 */
public class TypeConversionException extends RuntimeException {
    public TypeConversionException(String message) {
        super(message);
    }

    public TypeConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}