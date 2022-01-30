package kafka.connect.model;


import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroWriter<T extends SpecificRecord>  implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        SpecificRecord record = data;

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            SpecificDatumWriter<SpecificRecord> specificDatumWriter = new SpecificDatumWriter<SpecificRecord>(record.getSchema());
           Encoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(),outputStream);
           specificDatumWriter.write(record, encoder);
            encoder.flush();
            byte[] bytes = outputStream.toByteArray();
           return bytes;
        } catch (IOException e) {
            e.printStackTrace();
        }


        return new byte[0];
    }
}