package io.debezium.server.iceberg.typeconversion;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to handle type conversions between different data types
 * with validation to ensure data integrity during conversion.
 */
public class TypeConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TypeConverter.class);

    /**
     * Attempts to convert a value from one type to another while preserving data integrity
     *
     * @param value The value to convert
     * @param sourceType The original type of the value
     * @param targetType The desired type to convert to
     * @return The converted value
     * @throws TypeConversionException if the conversion is not possible or would result in data loss
     */
    public static Object convert(Object value, Type sourceType, Type targetType) throws TypeConversionException {
        if (value == null) {
            return null;
        }

        // If types are the same, no conversion needed
        if (sourceType.equals(targetType)) {
            return value;
        }

        try {
            // Handle float to int conversion
            if (sourceType.typeId() == Type.TypeID.DOUBLE && targetType.typeId() == Type.TypeID.INTEGER) {
                return convertFloatToInt(value);
            }

            // Handle int to boolean conversion
            if (sourceType.typeId() == Type.TypeID.INTEGER && targetType.typeId() == Type.TypeID.BOOLEAN) {
                return convertIntToBoolean(value);
            }

            // Handle string to int conversion
            if (sourceType.typeId() == Type.TypeID.STRING && targetType.typeId() == Type.TypeID.INTEGER) {
                return convertStringToInt(value);
            }

            // Handle int to string conversion
            if (sourceType.typeId() == Type.TypeID.INTEGER && targetType.typeId() == Type.TypeID.STRING) {
                return convertIntToString(value);
            }

            // Add more type conversion cases as needed

            throw new TypeConversionException(
                    String.format("Unsupported type conversion from %s to %s", sourceType, targetType));
        } catch (TypeConversionException e) {
            throw e;
        } catch (Exception e) {
            throw new TypeConversionException(
                    String.format("Failed to convert value from %s to %s: %s", sourceType, targetType, e.getMessage()));
        }
    }

    private static Integer convertFloatToInt(Object value) throws TypeConversionException {
        if (value instanceof Double) {
            double doubleValue = (Double) value;
            // Check if the double has no decimal part
            if (doubleValue == Math.floor(doubleValue)) {
                return (int) doubleValue;
            }
            throw new TypeConversionException(
                    String.format("Cannot convert float %f to int without losing decimal places", doubleValue));
        }
        throw new TypeConversionException("Value is not a float/double");
    }

    private static Boolean convertIntToBoolean(Object value) throws TypeConversionException {
        if (value instanceof Integer) {
            int intValue = (Integer) value;
            if (intValue == 1) {
                return true;
            } else if (intValue == 0) {
                return false;
            }
            throw new TypeConversionException(
                    String.format("Cannot convert int %d to boolean. Only 0 and 1 are valid values", intValue));
        }
        throw new TypeConversionException("Value is not an integer");
    }

    private static Integer convertStringToInt(Object value) throws TypeConversionException {
        if (value instanceof String) {
            String strValue = (String) value;
            try {
                return Integer.parseInt(strValue);
            } catch (NumberFormatException e) {
                throw new TypeConversionException(
                        String.format("Cannot convert string '%s' to int - not a valid integer", strValue));
            }
        }
        throw new TypeConversionException("Value is not a string");
    }

    private static String convertIntToString(Object value) {
        if (value instanceof Integer) {
            return value.toString();
        }
        throw new TypeConversionException("Value is not an integer");
    }
}