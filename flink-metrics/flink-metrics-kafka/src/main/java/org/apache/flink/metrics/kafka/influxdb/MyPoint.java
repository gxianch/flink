package org.apache.flink.metrics.kafka.influxdb;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.NumberFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/** 改造自Point 移除引入的influxdb相关的类和方法 */
public class MyPoint {
    private String measurement;
    private Map<String, String> tags;
    private Long time;
    private TimeUnit precision;
    private Map<String, Object> fields;
    private static final int MAX_FRACTION_DIGITS = 340;
    private static final ThreadLocal<NumberFormat> NUMBER_FORMATTER =
            ThreadLocal.withInitial(
                    () -> {
                        NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
                        numberFormat.setMaximumFractionDigits(340);
                        numberFormat.setGroupingUsed(false);
                        numberFormat.setMinimumFractionDigits(1);
                        return numberFormat;
                    });
    private static final int DEFAULT_STRING_BUILDER_SIZE = 1024;
    private static final ThreadLocal<StringBuilder> CACHED_STRINGBUILDERS =
            ThreadLocal.withInitial(
                    () -> {
                        return new StringBuilder(1024);
                    });

    public String getMeasurement() {
        return measurement;
    }

    MyPoint() {
        this.precision = TimeUnit.NANOSECONDS;
    }

    public static org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder measurement(
            String measurement) {
        return new org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder(measurement);
    }

    void setMeasurement(String measurement) {
        this.measurement = measurement;
    }

    void setTime(Long time) {
        this.time = time;
    }

    public Long getTime() {
        return time;
    }

    void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Map<String, String> getTags() {
        return this.tags;
    }

    void setPrecision(TimeUnit precision) {
        this.precision = precision;
    }

    public Map<String, Object> getFields() {
        return this.fields;
    }

    void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            org.apache.flink.metrics.kafka.influxdb.MyPoint point =
                    (org.apache.flink.metrics.kafka.influxdb.MyPoint) o;
            return Objects.equals(this.measurement, point.measurement)
                    && Objects.equals(this.tags, point.tags)
                    && Objects.equals(this.time, point.time)
                    && this.precision == point.precision
                    && Objects.equals(this.fields, point.fields);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(
                new Object[] {this.measurement, this.tags, this.time, this.precision, this.fields});
    }

    public String toString() {

        StringBuilder builder = new StringBuilder();
        builder.append("Point [name=");
        builder.append(this.measurement);
        if (this.time != null) {
            builder.append(", time=");
            builder.append(this.time);
        }

        builder.append(", tags=");
        builder.append(this.tags);
        if (this.precision != null) {
            builder.append(", precision=");
            builder.append(this.precision);
        }

        builder.append(", fields=");
        builder.append(this.fields);
        builder.append("]");
        return builder.toString();
    }

    public String lineProtocol() {
        return this.lineProtocol((TimeUnit) null);
    }

    public String lineProtocol(TimeUnit precision) {
        StringBuilder sb = (StringBuilder) CACHED_STRINGBUILDERS.get();
        sb.setLength(0);
        escapeKey(sb, this.measurement);
        this.concatenatedTags(sb);
        int writtenFields = this.concatenatedFields(sb);
        if (writtenFields == 0) {
            return "";
        } else {
            this.formatedTime(sb, precision);
            return sb.toString();
        }
    }

    private void concatenatedTags(StringBuilder sb) {
        Iterator var2 = this.tags.entrySet().iterator();

        while (var2.hasNext()) {
            Entry<String, String> tag = (Entry) var2.next();
            sb.append(',');
            escapeKey(sb, (String) tag.getKey());
            sb.append('=');
            escapeKey(sb, (String) tag.getValue());
        }

        sb.append(' ');
    }

    private int concatenatedFields(StringBuilder sb) {
        int fieldCount = 0;
        Iterator var3 = this.fields.entrySet().iterator();

        while (true) {
            Entry field;
            Object value;
            do {
                do {
                    if (!var3.hasNext()) {
                        int lengthMinusOne = sb.length() - 1;
                        if (sb.charAt(lengthMinusOne) == ',') {
                            sb.setLength(lengthMinusOne);
                        }

                        return fieldCount;
                    }

                    field = (Entry) var3.next();
                    value = field.getValue();
                } while (value == null);
            } while (isNotFinite(value));

            escapeKey(sb, (String) field.getKey());
            sb.append('=');
            if (value instanceof Number) {
                if (!(value instanceof Double)
                        && !(value instanceof Float)
                        && !(value instanceof BigDecimal)) {
                    sb.append(value).append('i');
                } else {
                    sb.append(((NumberFormat) NUMBER_FORMATTER.get()).format(value));
                }
            } else if (value instanceof String) {
                String stringValue = (String) value;
                sb.append('"');
                escapeField(sb, stringValue);
                sb.append('"');
            } else {
                sb.append(value);
            }

            sb.append(',');
            ++fieldCount;
        }
    }

    static void escapeKey(StringBuilder sb, String key) {
        int i = 0;

        while (i < key.length()) {
            switch (key.charAt(i)) {
                case ' ':
                case ',':
                case '=':
                    sb.append('\\');
                default:
                    sb.append(key.charAt(i));
                    ++i;
            }
        }
    }

    static void escapeField(StringBuilder sb, String field) {
        int i = 0;

        while (i < field.length()) {
            switch (field.charAt(i)) {
                case '"':
                case '\\':
                    sb.append('\\');
                default:
                    sb.append(field.charAt(i));
                    ++i;
            }
        }
    }

    private static boolean isNotFinite(Object value) {
        return value instanceof Double && !Double.isFinite((Double) value)
                || value instanceof Float && !Float.isFinite((Float) value);
    }

    private void formatedTime(StringBuilder sb, TimeUnit precision) {
        if (this.time != null) {
            if (precision == null) {
                sb.append(" ").append(TimeUnit.NANOSECONDS.convert(this.time, this.precision));
            } else {
                sb.append(" ").append(precision.convert(this.time, this.precision));
            }
        }
    }

    public static final class Builder {
        private final String measurement;
        private final Map<String, String> tags = new TreeMap();
        private Long time;
        private TimeUnit precision;
        private final Map<String, Object> fields = new TreeMap();

        Builder(String measurement) {
            this.measurement = measurement;
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder tag(
                String tagName, String value) {
            Objects.requireNonNull(tagName, "tagName");
            Objects.requireNonNull(value, "value");
            if (!tagName.isEmpty() && !value.isEmpty()) {
                this.tags.put(tagName, value);
            }

            return this;
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder tag(
                Map<String, String> tagsToAdd) {
            Iterator var2 = tagsToAdd.entrySet().iterator();

            while (var2.hasNext()) {
                Entry<String, String> tag = (Entry) var2.next();
                this.tag((String) tag.getKey(), (String) tag.getValue());
            }

            return this;
        }

        /** @deprecated */
        @Deprecated
        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder field(
                String field, Object value) {
            if (value instanceof Number) {
                if (value instanceof Byte) {
                    value = ((Byte) value).doubleValue();
                } else if (value instanceof Short) {
                    value = ((Short) value).doubleValue();
                } else if (value instanceof Integer) {
                    value = ((Integer) value).doubleValue();
                } else if (value instanceof Long) {
                    value = ((Long) value).doubleValue();
                } else if (value instanceof BigInteger) {
                    value = ((BigInteger) value).doubleValue();
                }
            }

            this.fields.put(field, value);
            return this;
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder addField(
                String field, boolean value) {
            this.fields.put(field, value);
            return this;
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder addField(
                String field, long value) {
            this.fields.put(field, value);
            return this;
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder addField(
                String field, double value) {
            this.fields.put(field, value);
            return this;
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder addField(
                String field, Number value) {
            this.fields.put(field, value);
            return this;
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder addField(
                String field, String value) {
            Objects.requireNonNull(value, "value");
            this.fields.put(field, value);
            return this;
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder fields(
                Map<String, Object> fieldsToAdd) {
            this.fields.putAll(fieldsToAdd);
            return this;
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint.Builder time(
                long timeToSet, TimeUnit precisionToSet) {
            Objects.requireNonNull(precisionToSet, "precisionToSet");
            this.time = timeToSet;
            this.precision = precisionToSet;
            return this;
        }

        public boolean hasFields() {
            return !this.fields.isEmpty();
        }

        public org.apache.flink.metrics.kafka.influxdb.MyPoint build() {
            org.apache.flink.metrics.kafka.influxdb.MyPoint point =
                    new org.apache.flink.metrics.kafka.influxdb.MyPoint();
            point.setFields(this.fields);
            point.setMeasurement(this.measurement);
            if (this.time != null) {
                point.setTime(this.time);
                point.setPrecision(this.precision);
            }

            point.setTags(this.tags);
            return point;
        }
    }
}
