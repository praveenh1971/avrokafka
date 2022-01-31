package kafka.connect.model;


import kafka.connect.KafkaWriter;
import kafka.connect.model.avro.Employee;
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
import java.util.logging.Logger;

public class AvroWriter<T extends SpecificRecord>  implements Serializer<T> {
    Logger logger = Logger.getLogger(AvroWriter.class.getCanonicalName());
    @Override
    public byte[] serialize(String topic, T data) {
        SpecificRecord record = data;

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            SpecificDatumWriter<SpecificRecord> specificDatumWriter = new SpecificDatumWriter<SpecificRecord>(record.getSchema());
           Encoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(),outputStream);
           specificDatumWriter.write(data, encoder);
            encoder.flush();
            byte[] bytes = outputStream.toByteArray();
            logger.info("Wrote record " + bytes.length);
           return bytes;
        } catch (IOException e) {
            e.printStackTrace();
        }


        return new byte[0];
    }
}
