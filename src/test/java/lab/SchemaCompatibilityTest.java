package lab;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lab.payments.PaymentCaptured;
import lab.support.Infra;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Registry compatibility POLICY tests for schemas/payment-captured.v*.avsc against a real Schema Registry (REST API).
 * Mode FULL_TRANSITIVE: every new version must read and be readable by ALL earlier versions.
 * Scope limit: the services send JSON; this does not prove the running producer/consumer honour the schema.
 */
class SchemaCompatibilityTest {

    static final ObjectMapper JSON = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newHttpClient();
    String subject;

    static ObjectNode load(int v) throws Exception {
        return (ObjectNode) JSON.readTree(Files.readString(Path.of("schemas/payment-captured.v" + v + ".avsc")));
    }

    static ObjectNode variant(ObjectNode base, Consumer<ArrayNode> change) {
        ObjectNode copy = base.deepCopy();
        change.accept((ArrayNode) copy.get("fields"));
        return copy;
    }

    static JsonNode call(String method, String path, Object body) throws Exception {
        var req = HttpRequest.newBuilder(URI.create(Infra.schemaRegistryUrl() + path))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                .build();
        HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        ObjectNode out = (ObjectNode) JSON.readTree(res.body().isEmpty() ? "{}" : res.body());
        out.put("_status", res.statusCode());
        return out;
    }

    static Object avro(ObjectNode schema) throws Exception {
        return Map.of("schemaType", "AVRO", "schema", JSON.writeValueAsString(schema));
    }

    JsonNode register(ObjectNode schema) throws Exception {
        return call("POST", "/subjects/" + subject + "/versions", avro(schema));
    }

    boolean compatible(ObjectNode schema) throws Exception {
        return call("POST", "/compatibility/subjects/" + subject + "/versions/latest", avro(schema)).get("is_compatible").asBoolean();
    }

    int latestVersion() throws Exception {
        return call("GET", "/subjects/" + subject + "/versions/latest", null).get("version").asInt();
    }

    @BeforeEach
    void registerV1() throws Exception {
        subject = "payments.captured." + UUID.randomUUID().toString().substring(0, 8) + "-value"; // TopicNameStrategy
        assertThat(register(load(1)).get("_status").asInt()).isEqualTo(200);
        assertThat(call("PUT", "/config/" + subject, Map.of("compatibility", "FULL_TRANSITIVE")).get("_status").asInt()).isEqualTo(200);
    }

    @Test
    void fieldNameAlignmentV1FieldNamesEqualTheJavaRecordComponents() throws Exception {
        List<String> fields = new ArrayList<>();
        load(1).get("fields").forEach(f -> fields.add(f.get("name").asText()));
        var components = Stream.of(PaymentCaptured.class.getRecordComponents()).map(c -> c.getName()).toList();
        assertThat(fields).containsExactlyInAnyOrderElementsOf(components);
    }

    @Test
    void v2AddsAnOptionalFieldWithADefaultCompatibleRegisteredAsVersion2() throws Exception {
        assertThat(compatible(load(2))).isTrue();
        assertThat(register(load(2)).get("_status").asInt()).isEqualTo(200);
        assertThat(latestVersion()).isEqualTo(2);
    }

    static Stream<Arguments> breakingChanges() throws Exception {
        ObjectNode v1 = load(1);
        return Stream.of(
                Arguments.of("adds a required field without a default",
                        variant(v1, f -> f.addObject().put("name", "merchantName").put("type", "string"))),
                Arguments.of("removes amountMinor",
                        variant(v1, f -> f.remove(indexOf(f, "amountMinor")))),
                Arguments.of("changes amountMinor from long to string",
                        variant(v1, f -> ((ObjectNode) f.get(indexOf(f, "amountMinor"))).put("type", "string"))));
    }

    static int indexOf(ArrayNode fields, String name) {
        for (int i = 0; i < fields.size(); i++) if (fields.get(i).get("name").asText().equals(name)) return i;
        throw new IllegalArgumentException(name);
    }

    @ParameterizedTest(name = "breaking change rejected by the registry: {0}")
    @MethodSource("breakingChanges")
    void breakingChangeRejected(String name, ObjectNode schema) throws Exception {
        assertThat(compatible(schema)).isFalse();
        assertThat(register(schema).get("_status").asInt()).isEqualTo(409);
        assertThat(latestVersion()).isEqualTo(1);
    }

    @Test
    void theModeIsAProductDecisionBackwardAloneWouldAcceptRemovingAmountMinor() throws Exception {
        call("PUT", "/config/" + subject, Map.of("compatibility", "BACKWARD"));
        assertThat(compatible(variant(load(1), f -> f.remove(indexOf(f, "amountMinor"))))).isTrue();
    }
}
