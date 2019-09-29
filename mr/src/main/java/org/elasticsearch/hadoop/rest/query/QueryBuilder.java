package org.elasticsearch.hadoop.rest.query;


import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;

/**
 * QueryBuilder for Elasticsearch query DSL
 */
public abstract class QueryBuilder {
    /**
     * Converts this QueryBuilder to a JSON string.
     *
     * @param out The JSON generator
     * @return The JSON string which represents the query
     */
    public abstract void toJson(Generator out);

    @Override
    public String toString() {
        FastByteArrayOutputStream out = new FastByteArrayOutputStream(256);
        JacksonJsonGenerator generator = new JacksonJsonGenerator(out);
        generator.writeBeginObject();
        toJson(generator);
        generator.writeEndObject();
        generator.close();
        return out.toString();
    }
}
