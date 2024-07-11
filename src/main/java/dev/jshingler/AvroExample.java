package dev.jshingler;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericData;

public class AvroExample {

    public static void main(String[] args) {
        String schemaJson = """
                {
                  "type": "record",
                  "name": "Example",
                  "fields": [
                    {"name": "custId", "type": "string"},
                    {
                      "name": "sessionId",
                      "type": {
                        "type": "string",
                        "logicalType": "validated-string",
                        "pattern": "^[0-9]{4}-[0-9]{2}$"
                      }
                    }
                  ]
                }""";

        LogicalTypes.register(ValidatedString.VALIDATED_STRING_LOGICAL_TYPE, new LogicalTypes.LogicalTypeFactory() {
            private final LogicalType validatedString = new ValidatedString();
            @Override
            public LogicalType fromSchema(Schema schema) {
                return validatedString;
            }
        });

        // Register the custom logical type
//        ValidatedString.register();
//        ValidatedString.addConversion();

//        LogicalTypes.register(ValidatedString.VALIDATED_STRING_LOGICAL_TYPE, ValidatedString::new);
        Schema schema = new Schema.Parser().parse(schemaJson);
//        LogicalTypes.register(ValidatedString.VALIDATED_STRING_LOGICAL_TYPE, new LogicalTypes.LogicalTypeFactory() {
//            private final LogicalType validatedString = new ValidatedString(schema.getField("sessionId").schema());
//            @Override
//            public LogicalType fromSchema(Schema schema) {
//                return validatedString;
//            }
//        });



//        LogicalTypes.register("ValidatedString.VALIDATED_STRING_LOGICAL_TYPE, ValidatedString::new);

        GenericData.get().addLogicalTypeConversion(new ValidatedString.ValidatedStringConversion(new ValidatedString()));

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("custId", "abc");
        builder.set("sessionId", "1234-aa");


        GenericRecord record = builder.build();
//        GenericData gd = GenericData.get();
//        boolean b = gd.validate(schema, record);
        System.out.println("Record is valid: " + record);
    }
}

