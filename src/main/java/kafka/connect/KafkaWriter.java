package kafka.connect;

import kafka.connect.model.AvroReader;
import kafka.connect.model.AvroWriter;
import kafka.connect.model.avro.Employee;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class KafkaWriter {
    public static void main(String[] args) {
            Logger logger = Logger.getLogger(KafkaWriter.class.getCanonicalName());
//            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        Employee employee = Employee.newBuilder().setAge(11).setName("AArush").setLastName("Marchareddy").setPhoneNumber("6094249761").build();
//
//        SpecificDatumWriter<Employee> specificDatumWriter = new SpecificDatumWriter<Employee>(employee.getSchema());
//        Encoder encoder = EncoderFactory.get().jsonEncoder(employee.getSchema(),outputStream);
//        specificDatumWriter.write(employee, encoder);
//
//            encoder.flush();
//            byte[] bytes = outputStream.toByteArray();
//            SpecificDatumReader<Employee>  datumReader = new SpecificDatumReader<Employee>(employee.getSchema());
//            Decoder decoder = DecoderFactory.get().jsonDecoder(employee.getSchema(), new ByteArrayInputStream(bytes));
//            Employee record = datumReader.read(null, decoder);
//            logger.info(record.toString());
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroWriter.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroReader.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "emp11");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        ArrayList<Employee> employees = new ArrayList<Employee>();
        employees.add ( Employee.newBuilder().setAge(11).setName("AArush").setLastName("Marchareddy").setPhoneNumber("6094249761").build());
        employees.add ( Employee.newBuilder().setAge(11).setName("Arnav").setLastName("Marchareddy").setPhoneNumber("6094249761").build());
        employees.add ( Employee.newBuilder().setAge(33).setName("Leena").setLastName("Marchareddy").setPhoneNumber("6094249761").build());
        employees.add ( Employee.newBuilder().setAge(44).setName("Praveen").setLastName("Marchareddy").setPhoneNumber("6094249761").build());


        KafkaProducer producer = new KafkaProducer(props);
//
        try {

            employees.forEach(ec->{
                logger.info("Sending " + ec.getName());
                producer.send(new ProducerRecord("employeesname", ec.getName(), ec.getName()), (x, e)->{
                    logger.info("Sent  " + x +  " e " + e);
                });
                producer.flush();
            });


        } catch (Throwable e) {
            logger.info(e.getMessage());
        }

        logger.info("CONSUMER Starting");
        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        ArrayList<String> arrayList = new ArrayList<String>();
        arrayList.add("newemployees");
        kafkaConsumer.subscribe(arrayList);
        ConsumerRecords records = kafkaConsumer.poll(Duration.ofMillis(1000));
        try {
            records.forEach(d -> {
                logger.info("record read" + d.toString());

            });
            logger.info("CONSUMER Started");
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
