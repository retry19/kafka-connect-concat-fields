package com.github.retry19.kafka.connect;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ConcatFields<R extends ConnectRecord<R>> implements Transformation<R> {
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private interface ConfigName {
        String SOURCE_FIELDS = "source.fields";
        String TARGET_FIELD = "target.field";
        String SEPARATOR = "separator";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(ConfigName.SOURCE_FIELDS, ConfigDef.Type.STRING, "id", ConfigDef.Importance.HIGH, "Fields source will conceited")
        .define(ConfigName.TARGET_FIELD, ConfigDef.Type.STRING, "id", ConfigDef.Importance.HIGH, "Field for place conceited result")
        .define(ConfigName.SEPARATOR, ConfigDef.Type.STRING, "+", ConfigDef.Importance.LOW, "Separator between conceited fields");

    private static final String PURPOSE = "Concatenating fields";
    private String[] sourceFields;
    private String targetField;
    private String separator;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        }
        return applyWithSchema(record);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            final SchemaBuilder schemaBuilder = SchemaUtil.copySchemaBasics(value.schema(), SchemaBuilder.struct());
            for (Field field: value.schema().fields()) {
                schemaBuilder.field(field.name(), field.schema());
            }

            schemaBuilder.field(targetField, Schema.STRING_SCHEMA);

            updatedSchema = schemaBuilder.build();
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field: value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        StringJoiner fieldJoiner = new StringJoiner(separator);
        for (String sourceField: sourceFields) {
            fieldJoiner.add(value.getString(sourceField));
        }

        updatedValue.put(targetField, fieldJoiner.toString());

        return newRecord(record, updatedSchema, updatedValue);
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);

        StringJoiner fieldJoiner = new StringJoiner(separator);
        for (String sourceField: sourceFields) {
            fieldJoiner.add(value.get(sourceField).toString());
        }

        updatedValue.put(targetField, fieldJoiner.toString());

        return newRecord(record, null, updatedValue);
    }

    protected abstract Object operatingValue(R record);

    protected abstract Schema operatingSchema(R record);

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        sourceFields = config.getString(ConfigName.SOURCE_FIELDS).split(",", 0);
        targetField = config.getString(ConfigName.TARGET_FIELD);
        separator = config.getString(ConfigName.SEPARATOR);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    public static class Key<R extends ConnectRecord<R>> extends ConcatFields<R> {
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                updatedSchema,
                updatedValue,
                record.valueSchema(),
                record.value(),
                record.timestamp()
            );
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ConcatFields<R> {
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
            );
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }
    }
}
