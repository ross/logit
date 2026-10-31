// The transactional producer of `script/kafka-interop`'s `consume-read-committed` leg: 10
// transactions of 10 records each to one topic, the even ones committed and the odd ones aborted.
// Each value names its fate, transaction, and record: `committed 4 7`, `aborted 5 0`.
//
// Usage: java -cp '/opt/kafka/libs/*:<dir with Txn.class>' Txn <bootstrap> <topic>
//
// An aborted transaction's records are flushed before the abort, so they reach the log and a
// `read_uncommitted` consumer reads them; abortTransaction alone may discard records still in the
// producer's buffer.

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Txn {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("transactional.id", "kafka-interop-txn");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();
            for (int txn = 0; txn < 10; txn++) {
                boolean commit = txn % 2 == 0;
                producer.beginTransaction();
                for (int record = 0; record < 10; record++) {
                    String value = (commit ? "committed " : "aborted ") + txn + " " + record;
                    producer.send(new ProducerRecord<>(args[1], value));
                }
                producer.flush();
                if (commit) {
                    producer.commitTransaction();
                } else {
                    producer.abortTransaction();
                }
            }
        }
        System.out.println("txn: 5 committed, 5 aborted");
    }
}
