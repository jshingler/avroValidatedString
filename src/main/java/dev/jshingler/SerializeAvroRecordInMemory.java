package dev.jshingler;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SerializeAvroRecordInMemory {
    public static void main(String[] args) throws IOException {
        // Define your Avro schema (replace with your actual schema)
        String schemaJson = """
                {
                  "type": "record",
                  "name": "User",
                  "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "email", "type": ["null", "string"], "default": null},
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

        // **IMPORTANT:** ValidatedString must be registered before parsing the schema
        // Could be done in application startup to minimize impact
        ValidatedString.register();

        Schema schema = new Schema.Parser().parse(schemaJson);

        // Create a generic Avro record (replace with your actual data)
        GenericRecord userRecord = new GenericData.Record(schema);
        userRecord.put("name", "Alice");
        userRecord.put("age", 30);
        userRecord.put("sessionId", "1234-9900000000");

        // ===================
        // Serialize the record
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        writer.write(userRecord, encoder);
        encoder.flush();
        out.close();

        System.out.println("Serialization successful.");
        // =============


        // Serialize the record to a byte array
        byte[] serializedRecord = serializeAvroRecord(schema, userRecord);

        // Print the serialized record (for demonstration)
        System.out.println("Serialized Avro record as byte array: " + serializedRecord.length + " bytes");

        // Deserialize the byte array back into an Avro record
        GenericRecord deserializedRecord = deserializeAvroRecord(schema, serializedRecord);

        // Print the deserialized record (for demonstration)
        System.out.println("Deserialized Avro record: " + deserializedRecord);
    }

    private static byte[] serializeAvroRecord(Schema schema, GenericRecord record) throws IOException {
        // Create Avro datum writer
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);

        // Create an output stream to hold the serialized data
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // Create Avro encoder that writes to the output stream
//        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        Encoder encoder = EncoderFactory.get().validatingEncoder(schema,EncoderFactory.get().binaryEncoder(outputStream, null));

        // Serialize record to the encoder
        datumWriter.write( record, encoder);

        // Flush the encoder (optional)
        encoder.flush();

        // Get the serialized bytes from the output stream
        byte[] serializedBytes = outputStream.toByteArray();

        // Close the output stream (optional, since it's ByteArrayOutputStream)
        outputStream.close();

        // Return the serialized bytes
        return serializedBytes;
    }

    private static GenericRecord deserializeAvroRecord(Schema schema, byte[] serializedBytes) throws IOException {
        // Create Avro datum reader
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);

        // Create Avro decoder from the serialized bytes
        Decoder decoder = DecoderFactory.get().validatingDecoder(schema,DecoderFactory.get().binaryDecoder(serializedBytes, null));

        // Deserialize record from the decoder
        return datumReader.read(null, decoder);
    }

}
