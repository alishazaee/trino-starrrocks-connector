package io.trino.plugin.starrocks;

import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Explicit type whitelist: unsupported JDBC mappings must stay on the JDBC path. */
final class StarRocksArrowReader
{
    private StarRocksArrowReader() {}

    static boolean supports(Type type)
    {
        return type == BooleanType.BOOLEAN || type == TinyintType.TINYINT || type == SmallintType.SMALLINT ||
                type == IntegerType.INTEGER || type == BigintType.BIGINT || type == RealType.REAL ||
                type == DoubleType.DOUBLE || type == DateType.DATE || type == VarbinaryType.VARBINARY ||
                type instanceof VarcharType || type instanceof CharType || type instanceof DecimalType ||
                (type instanceof TimestampType timestamp && timestamp.getPrecision() <= 6);
    }

    static void write(Type type, FieldVector vector, int row, BlockBuilder block)
    {
        if (vector.isNull(row)) {
            block.appendNull();
            return;
        }
        Object value = vector.getObject(row);
        if (type == BooleanType.BOOLEAN) {
            type.writeBoolean(block, value instanceof Boolean b ? b : ((Number) value).intValue() != 0);
        }
        else if (type == RealType.REAL) {
            type.writeLong(block, Float.floatToRawIntBits(((Number) value).floatValue()));
        }
        else if (type == DoubleType.DOUBLE) {
            type.writeDouble(block, ((Number) value).doubleValue());
        }
        else if (type instanceof DecimalType decimal) {
            BigDecimal number = ((BigDecimal) value).setScale(decimal.getScale());
            if (number.precision() > decimal.getPrecision()) {
                throw new ArithmeticException("Arrow decimal exceeds Trino precision");
            }
            if (decimal.isShort()) {
                decimal.writeLong(block, number.unscaledValue().longValueExact());
            }
            else {
                decimal.writeObject(block, Int128.valueOf(number.unscaledValue()));
            }
        }
        else if (type instanceof VarcharType || type instanceof CharType) {
            String text = value.toString();
            if (type instanceof CharType) {
                type.writeSlice(block, Chars.trimTrailingSpaces(Slices.utf8Slice(text)));
            }
            else {
                type.writeSlice(block, Slices.utf8Slice(text));
            }
        }
        else if (type == VarbinaryType.VARBINARY) {
            type.writeSlice(block, Slices.wrappedBuffer((byte[]) value));
        }
        else if (type == DateType.DATE) {
            long day = value instanceof LocalDate date ? date.toEpochDay() :
                    value instanceof LocalDateTime dateTime ? dateTime.toLocalDate().toEpochDay() : ((Number) value).longValue();
            type.writeLong(block, day);
        }
        else if (type instanceof TimestampType) {
            ArrowType.Timestamp arrowType = (ArrowType.Timestamp) vector.getField().getType();
            if (arrowType.getTimezone() != null && !arrowType.getTimezone().isEmpty() && !arrowType.getTimezone().equals("UTC")) {
                throw new IllegalArgumentException("Unexpected timezone in StarRocks DATETIME result");
            }
            // StarRocks encodes timezone-free DATETIME as Arrow microseconds tagged UTC.
            long raw = ((org.apache.arrow.vector.TimeStampVector) vector).get(row);
            long micros = switch (arrowType.getUnit()) {
                case SECOND -> Math.multiplyExact(raw, 1_000_000);
                case MILLISECOND -> Math.multiplyExact(raw, 1_000);
                case MICROSECOND -> raw;
                case NANOSECOND -> Math.floorDiv(raw, 1_000);
            };
            type.writeLong(block, micros);
        }
        else {
            type.writeLong(block, ((Number) value).longValue());
        }
    }
}
