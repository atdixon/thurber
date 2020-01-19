package thurber.java;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.Map;

import static java.util.Collections.emptyMap;

public interface TOptions extends PipelineOptions {

    @Default.InstanceFactory(DefaultValueFactoryImpl.class)
    Map<String, Object> getCustomConfig();

    void setCustomConfig(Map<String, Object> config);

    class DefaultValueFactoryImpl implements DefaultValueFactory<Map<String, Object>> {
        @Override public Map<String, Object> create(PipelineOptions options) {
            return emptyMap();
        }
    }

}
