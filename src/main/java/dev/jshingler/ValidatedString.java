package dev.jshingler;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

import java.util.regex.Pattern;

public class ValidatedString extends LogicalType {

    public static final String VALIDATED_STRING_LOGICAL_TYPE = "validated-string";
    private static final String PATTERN = "pattern";

    private Pattern pattern;
    private Schema schema;

    public ValidatedString() {
        super(VALIDATED_STRING_LOGICAL_TYPE);
    }

    private ValidatedString(String pattern) {
        super(VALIDATED_STRING_LOGICAL_TYPE);
        this.pattern = Pattern.compile(pattern);
    }

    public ValidatedString(Schema schema) {
        super(VALIDATED_STRING_LOGICAL_TYPE);
        if (!hasProperty(schema, PATTERN)) {
            throw new IllegalArgumentException("Invalid validated string: missing pattern");
        }

        this.schema = schema;
        this.pattern = Pattern.compile(schema.getProp("pattern"));
    }

    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public Schema addToSchema(Schema schema) {
        super.addToSchema(schema);
        schema.addProp(PATTERN, pattern);
        return schema;
    }

    private boolean hasProperty(Schema schema, String name)  {
        return schema.getProp(name) != null;
    }

    @Override
    public void validate(Schema schema) {
        super.validate(schema);
        // validate the type
        if (schema.getType() != Schema.Type.STRING ) {
            throw new IllegalArgumentException("Logical type validated-string must be backed by string");
        }
        this.schema = schema;
        pattern = Pattern.compile(schema.getProp("pattern"));
        if (pattern == null) {
            throw new IllegalArgumentException("Invalid validated-string pattern: " + pattern + " (must be a regular expression)");
        }
    }

    public void validate(String value) {
        if (!pattern.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid String: " + value + " Expected: " + pattern.toString());
        }
    }

    public static void register() {
        LogicalTypes.register(ValidatedString.VALIDATED_STRING_LOGICAL_TYPE, new LogicalTypes.LogicalTypeFactory() {

            private final LogicalType validatedString = new ValidatedString();

            @Override
            public LogicalType fromSchema(Schema schema) {
                return validatedString;
            }
        });

        GenericData.get().addLogicalTypeConversion(new ValidatedString.ValidatedStringConversion(new ValidatedString()));
        SpecificData.get().addLogicalTypeConversion(new ValidatedString.ValidatedStringConversion(new ValidatedString()));
    }


    public static class ValidatedStringConversion extends Conversion<String> {
        private final ValidatedString logicalType;

        public ValidatedStringConversion(ValidatedString logicalType) {
            this.logicalType = logicalType;
        }

        @Override
        public Class<String> getConvertedType() {
            return String.class;
        }

        @Override
        public String getLogicalTypeName() {
            return logicalType.getName();
        }

        @Override
        public String fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
            String stringValue = value.toString();
            ((ValidatedString) type).validate(stringValue);
            return stringValue;
        }

        @Override
        public CharSequence toCharSequence(String value, Schema schema, LogicalType type) {
            ((ValidatedString) type).validate(value);
            return value;
        }

    }
}

