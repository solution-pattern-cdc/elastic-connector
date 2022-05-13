package org.acme.retail;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.camel.Exchange;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.elasticsearch.ElasticsearchConstants;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

public class ProductRouteTest extends CamelTestSupport {

    @Override
    public boolean isUseAdviceWith() {
        return true;
    }

    @Override
    protected Properties useOverridePropertiesWithPropertiesComponent() {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("src/test/resources/application.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;

    }

    void adviceWithRoute() throws Exception {
        AdviceWith.adviceWith(context, "kafka2Elastic", a -> {
            a.replaceFromWith("direct:start");
            a.weaveByToUri("elasticsearch-rest:*").replace().to("mock:elasticsearch-rest");
        });
        context.start();
    }

    @Test
    public void testProductRoute() throws Exception {

        adviceWithRoute();

        getMockEndpoint("mock:elasticsearch-rest").expectedMessageCount(1);

        template.sendBody("direct:start", getProductCreatedEvent());

        Exchange ex = getMockEndpoint("mock:elasticsearch-rest").assertExchangeReceived(0);
        MatcherAssert.assertThat(ex.getIn().getBody(), Matchers.notNullValue());
        MatcherAssert.assertThat(ex.getIn().getBody(), Matchers.instanceOf(IndexRequest.class));
        IndexRequest ir = ex.getIn().getBody(IndexRequest.class);
        MatcherAssert.assertThat(ir.id(), Matchers.equalTo("1109"));
        MatcherAssert.assertThat(XContentHelper.convertToJson(ir.source(), false, false, XContentType.JSON), Matchers.equalTo(getExpectedSource()));
        MatcherAssert.assertThat(ex.getIn().getHeader(ElasticsearchConstants.PARAM_INDEX_NAME), Matchers.equalTo(context.getPropertiesComponent().resolveProperty("elasticsearch.index.name").get()));
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new ProductRoute();
    }

    String getProductCreatedEvent() {
        return "{" +
                "   \"before\": null," +
                "   \"after\": {" +
                "      \"product_id\": 1109," +
                "      \"name\": \"Pont Yacht\"," +
                "      \"description\": \"Measures 38 inches Long x 33 3/4 inches High. Includes a stand. Many extras including rigging, long boats, pilot house, anchors, etc. Comes with 2 masts, all square-rigged\"," +
                "      \"price\": \"33.30\"" +
                "   }," +
                "   \"source\": {" +
                "      \"version\": \"1.7.0.Final\"," +
                "      \"connector\": \"postgresql\"," +
                "      \"name\": \"retail.updates\"," +
                "      \"ts_ms\": 1652344174613," +
                "      \"snapshot\": \"false\"," +
                "      \"db\": \"retail\"," +
                "      \"schema\": \"public\"," +
                "      \"table\": \"product\"," +
                "      \"txId\": 527," +
                "      \"lsn\": 23703032," +
                "      \"xmin\": null" +
                "   }," +
                "   \"op\": \"c\"," +
                "   \"ts_ms\": 1652344175077," +
                "   \"transaction\": null" +
                "}";
    }

    String getExpectedSource() {
        return "{\"name\":\"Pont Yacht\"," +
                "\"description\":\"Measures 38 inches Long x 33 3/4 inches High. Includes a stand. " +
                "Many extras including rigging, long boats, pilot house, anchors, etc. Comes with 2 masts, all square-rigged\"}";
    }
}
