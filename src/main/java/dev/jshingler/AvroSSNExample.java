package dev.jshingler;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class AvroSSNExample {
    public static void main(String[] args) throws Exception {
        // Define schema with an SSN field
        String schemaString = """
                {"type":"record",
                 "name":"TestRecord",
                "fields":[
                    {"name":"ssn",
                     "type": {
                             "type": "string",
                             "logicalType": "validated-string",
                             "pattern": "^[0-9]{3}-[0-9]{2}-[0-9]{4}$"
                           }}
                ]}""";

        // **IMPORTANT:** ValidatedString must be registered before parsing the schema
        // Could be done in application startup to minimize impact
        ValidatedString.register();

        Schema schema = new Schema.Parser().parse(schemaString);

        // Create a record with a valid SSN
        GenericRecord record = new GenericData.Record(schema);
        record.put("ssn", "123-45-678a");

        // Serialize the record
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();

        // Deserialize the record
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord deserializedRecord = reader.read(null, DecoderFactory.get().binaryDecoder(in, null));

        // Validate the deserialized data
        System.out.println("Deserialized SSN: " + deserializedRecord.get("ssn"));
    }
}
