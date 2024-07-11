package dev.jshingler;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MultiLineAvroConverter {

    public static void main(String[] args) {
        String schemaFilePath = "user.avsc";
        String inputFilePath = "input.txt";
        String outputFilePath = "./users.avro";

        // **IMPORTANT:** ValidatedString must be registered before parsing the schema
        // Could be done in application startup to minimize impact
        ValidatedString.register();

        Schema schema = null;

        // Step 1: Parse the schema
        try {
            schema = new Schema.Parser().parse(new File(schemaFilePath));
        } catch (IOException e) {
            System.err.println("Failed to parse schema file: " + schemaFilePath);
            e.printStackTrace();
            return;
        }

        // Step 2: Read the input file and create records
        List<GenericRecord> records = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
            String line;
            GenericRecord record = new GenericData.Record(schema);

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    // Blank line indicates end of record
                    if (!record.getSchema().getFields().isEmpty()) {
                        records.add(record);
                        record = new GenericData.Record(schema);
                    }
                } else {
                    String[] parts = line.split(":", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        try {
//                            validateField(key, value);
                            switch (key) {
                                case "name":
                                    record.put("name", value);
                                    break;
                                case "age":
                                    record.put("age", Integer.parseInt(value));
                                    break;
                                case "email":
                                    record.put("email", value.isEmpty() ? null : value);
                                    break;
                                case "sessionId":
                                    record.put("sessionId",  value);
                                    break;
                                default:
                                    System.err.println("Unknown field: " + key);
                            }
                        } catch (IllegalArgumentException e) {
                            System.err.println("Validation error for field " + key + ": " + e.getMessage());
                        }
                    } else {
                        System.err.println("Invalid line format: " + line);
                    }
                }
            }
            // Add the last record if the file does not end with a blank line
            if (!record.getSchema().getFields().isEmpty()) {
                records.add(record);
            }
        } catch (FileNotFoundException e) {
            System.err.println("Input file not found: " + inputFilePath);
            e.printStackTrace();
            return;
        } catch (IOException e) {
            System.err.println("Failed to read input file: " + inputFilePath);
            e.printStackTrace();
            return;
        }

        // Step 3: Write the records to an Avro file
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, new File(outputFilePath));
            for (GenericRecord rec : records) {
                dataFileWriter.append(rec);
            }
        } catch (IOException e) {
            System.err.println("Failed to write Avro file: " + outputFilePath);
            e.printStackTrace();
        }

        System.out.println("Avro file created successfully!");
    }

    private static void validateField(String key, String value) {
        switch (key) {
            case "name":
                if (value.isEmpty()) {
                    throw new IllegalArgumentException("Name cannot be empty");
                }
                break;
            case "age":
                try {
                    int age = Integer.parseInt(value);
                    if (age < 0) {
                        throw new IllegalArgumentException("Age cannot be negative");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Age must be an integer");
                }
                break;
            case "email":
                if (!value.isEmpty() && !value.contains("@")) {
                    throw new IllegalArgumentException("Invalid email format");
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown field: " + key);
        }
    }
}

