package org.acme.retail;

import java.util.HashMap;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;

import io.vertx.core.json.JsonObject;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.elasticsearch.ElasticsearchConstants;
import org.elasticsearch.action.index.IndexRequest;

@ApplicationScoped
public class ProductRoute extends RouteBuilder {

    @SuppressWarnings({"unchecked"})
    @Override
    public void configure() throws Exception {

        from("kafka:{{kafka.product.topic.name}}?groupId={{kafka.product.consumer.group}}" +
                "&autoOffsetReset=earliest")
                .routeId("kafka2Elastic")
                .unmarshal().json()
                .log(LoggingLevel.DEBUG, "Product event received: ${body}")
                .setHeader(ElasticsearchConstants.PARAM_INDEX_NAME, simple("${properties:elasticsearch.index.name}"))
                .process(exchange -> {
                    Message in = exchange.getIn();
                    JsonObject after = new JsonObject(in.getBody(Map.class)).getJsonObject("after");
                    Map<String, String> document = new HashMap<>();
                    document.put("name", after.getString("name"));
                    document.put("description", after.getString("description"));
                    IndexRequest request = new IndexRequest(in.getHeader(ElasticsearchConstants.PARAM_INDEX_NAME, String.class))
                            .id(String.valueOf(after.getLong("product_id"))).source(document);
                    in.setBody(request);
                })
                .to("elasticsearch-rest://retail");
    }
}
