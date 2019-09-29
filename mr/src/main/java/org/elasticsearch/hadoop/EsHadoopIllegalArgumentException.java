package org.elasticsearch.hadoop;

/**
 * EsHadoop 参数异常
 */
public class EsHadoopIllegalArgumentException extends EsHadoopException {

    public EsHadoopIllegalArgumentException() {
        super();
    }

    public EsHadoopIllegalArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsHadoopIllegalArgumentException(String message) {
        super(message);
    }

    public EsHadoopIllegalArgumentException(Throwable cause) {
        super(cause);
    }
}
