package thurber.java;

import org.apache.beam.sdk.options.PipelineOptions;

import java.util.Map;

public interface TOptions extends PipelineOptions {

    Map<String, Object> getCustomConfig();

    void setCustomConfig(Map<String, Object> config);

}
