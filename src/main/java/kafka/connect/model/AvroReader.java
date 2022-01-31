package kafka.connect.model;

import kafka.connect.model.avro.Employee;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class AvroReader implements Deserializer {
    Employee employee = new Employee();
    @Override
    public Object deserialize(String topic, byte[] data) {

        try {

            Decoder decoder = DecoderFactory.get().jsonDecoder(  employee.getSchema(),new ByteArrayInputStream(data));
            SpecificDatumReader<SpecificRecord> datumReader = new SpecificDatumReader(employee.getSchema());
            SpecificRecord record = datumReader.read(null, decoder);
            return  record;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
