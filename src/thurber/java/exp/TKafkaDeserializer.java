package thurber.java.exp;

import clojure.lang.Var;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import thurber.java.Core;

import java.util.Map;

public final class TKafkaDeserializer implements Deserializer<Object> {

    private volatile Deserializer<Object> delegate;

    @Override public void configure(Map<String, ?> configs, boolean isKey) {
        Var var = (Var) configs.get(
            isKey ? "thurber.kafka-key-deserializer" : "thurber.kafka-value-deserializer");
        Core.require_(var);
        //noinspection unchecked
        this.delegate = (Deserializer<Object>) var.deref();
    }

    @Override public Object deserialize(String topic, byte[] data) {
        return this.delegate.deserialize(topic, data);
    }

    @Override public Object deserialize(String topic, Headers headers, byte[] data) {
        return this.delegate.deserialize(topic, headers, data);
    }

    @Override public void close() {
        this.delegate.close();;
    }

}
