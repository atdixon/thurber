package thurber.java.exp;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * <code>
 * (def fn-impl
 *   (proxy [AbstractDoFn] []
 *     (processElement [^DoFn$ProcessContext context elem]
 *       (let [[{:keys [prefix]}] (th/*proxy-args)]
 *         (.output context (str prefix elem))))))
 * <br>
 * <br>
 * (ParDo/of (th/proxy* #'fn-impl {:prefix "tag: "}))
 * </code>
 */
public abstract class AbstractDoFn extends DoFn<Object, Object> {

    @ProcessElement
    public abstract void processElement(ProcessContext context, @Element Object val);

}
