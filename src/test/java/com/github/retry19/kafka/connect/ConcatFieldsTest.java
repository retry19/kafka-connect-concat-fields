package com.github.retry19.kafka.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ConcatFieldsTest {
    private final ConcatFields<SourceRecord> formValue = new ConcatFields.Value<>();

    @After
    public void tearDown() {
        formValue.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        Map<String, String> props = new HashMap<>();
        props.put("source.fields", "WalletId,CardId");
        props.put("target.field", "id");
        formValue.configure(props);

        final Schema valueSchema = SchemaBuilder.struct()
            .name("Value-schema")
            .doc("doc")
            .field("WalletId", Schema.STRING_SCHEMA)
            .field("CardId", Schema.STRING_SCHEMA)
            .build();
        final Struct value = new Struct(valueSchema)
            .put("WalletId", 33L)
            .put("CardId", 123L);

        formValue.apply(new SourceRecord(null, null, "", 0, valueSchema, value));
    }

    @Test
    public void updateNewSchema() {
        final Map<String, Object> props = new HashMap<>();
        props.put("source.fields", "WalletId,CardId");

        formValue.configure(props);

        final Schema valueSchema = SchemaBuilder.struct().name("example").version(1).doc("doc")
            .field("WalletId", Schema.STRING_SCHEMA)
            .field("CardId", Schema.STRING_SCHEMA)
            .build();
        final Struct value = new Struct(valueSchema)
            .put("WalletId", "abc")
            .put("CardId", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, valueSchema, value);
        final SourceRecord transformedRecord = formValue.apply(record);

        Assert.assertEquals(valueSchema.name(), transformedRecord.valueSchema().name());
        Assert.assertEquals(valueSchema.version(), transformedRecord.valueSchema().version());
        Assert.assertEquals(valueSchema.doc(), transformedRecord.valueSchema().doc());
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("WalletId").schema());
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("CardId").schema());
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("id").schema());

        final SourceRecord transformedRecord2 = formValue.apply(
            new SourceRecord(
                null,
                null,
                "test",
                1,
                valueSchema,
                new Struct(valueSchema)
                    .put("WalletId", "ghi")
                    .put("CardId", "jkl")
            )
        );
        Assert.assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());
    }

    @Test
    public void concatFields() {
        final Map<String, Object> props = new HashMap<>();
        props.put("source.fields", "WalletId,CardId");

        formValue.configure(props);

        final Schema valueSchema = SchemaBuilder.struct().name("example").version(1).doc("doc")
                .field("WalletId", Schema.STRING_SCHEMA)
                .field("CardId", Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("WalletId", "abc")
                .put("CardId", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, valueSchema, value);
        final SourceRecord transformedRecord = formValue.apply(record);

        Assert.assertEquals("abc", ((Struct) transformedRecord.value()).getString("WalletId"));
        Assert.assertEquals("def", ((Struct) transformedRecord.value()).getString("CardId"));
        Assert.assertEquals("abc+def", ((Struct) transformedRecord.value()).getString("id"));
    }

    @Test
    public void concatFieldsUsingCustomTargetField() {
        final Map<String, Object> props = new HashMap<>();
        props.put("source.fields", "WalletId,CardId");
        props.put("target.field", "DocumentId");

        formValue.configure(props);

        final Schema valueSchema = SchemaBuilder.struct().name("example").version(1).doc("doc")
                .field("WalletId", Schema.STRING_SCHEMA)
                .field("CardId", Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("WalletId", "abc")
                .put("CardId", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, valueSchema, value);
        final SourceRecord transformedRecord = formValue.apply(record);

        Assert.assertEquals("abc", ((Struct) transformedRecord.value()).getString("WalletId"));
        Assert.assertEquals("def", ((Struct) transformedRecord.value()).getString("CardId"));
        Assert.assertEquals("abc+def", ((Struct) transformedRecord.value()).getString("DocumentId"));
    }

    @Test
    public void concatFieldsUsingCustomSeparator() {
        final Map<String, Object> props = new HashMap<>();
        props.put("source.fields", "WalletId,CardId");
        props.put("separator", "--");

        formValue.configure(props);

        final Schema valueSchema = SchemaBuilder.struct().name("example").version(1).doc("doc")
                .field("WalletId", Schema.STRING_SCHEMA)
                .field("CardId", Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("WalletId", "abc")
                .put("CardId", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, valueSchema, value);
        final SourceRecord transformedRecord = formValue.apply(record);

        Assert.assertEquals("abc", ((Struct) transformedRecord.value()).getString("WalletId"));
        Assert.assertEquals("def", ((Struct) transformedRecord.value()).getString("CardId"));
        Assert.assertEquals("abc--def", ((Struct) transformedRecord.value()).getString("id"));
    }

    @Test
    public void schemalessConcatFields() {
        final Map<String, Object> props = new HashMap<>();
        props.put("source.fields", "WalletId,CardId");

        formValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("WalletId", "abc");
        value.put("CardId", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, value);
        final SourceRecord transformedRecord = formValue.apply(record);

        Assert.assertEquals("abc", ((Map<?, ?>) transformedRecord.value()).get("WalletId"));
        Assert.assertEquals("def", ((Map<?, ?>) transformedRecord.value()).get("CardId"));
        Assert.assertEquals("abc+def", ((Map<?, ?>) transformedRecord.value()).get("id"));
    }

    @Test
    public void schemalessConcatFieldsUsingCustomTargetField() {
        final Map<String, Object> props = new HashMap<>();
        props.put("source.fields", "WalletId,CardId");
        props.put("target.field", "DocId");

        formValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("WalletId", "abc");
        value.put("CardId", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, value);
        final SourceRecord transformedRecord = formValue.apply(record);

        Assert.assertEquals("abc", ((Map<?, ?>) transformedRecord.value()).get("WalletId"));
        Assert.assertEquals("def", ((Map<?, ?>) transformedRecord.value()).get("CardId"));
        Assert.assertEquals("abc+def", ((Map<?, ?>) transformedRecord.value()).get("DocId"));
    }

    @Test
    public void schemalessConcatFieldsUsingCustomSeparator() {
        final Map<String, Object> props = new HashMap<>();
        props.put("source.fields", "WalletId,CardId");
        props.put("separator", "-");

        formValue.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("WalletId", "abc");
        value.put("CardId", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, value);
        final SourceRecord transformedRecord = formValue.apply(record);

        Assert.assertEquals("abc", ((Map<?, ?>) transformedRecord.value()).get("WalletId"));
        Assert.assertEquals("def", ((Map<?, ?>) transformedRecord.value()).get("CardId"));
        Assert.assertEquals("abc-def", ((Map<?, ?>) transformedRecord.value()).get("id"));
    }
}
