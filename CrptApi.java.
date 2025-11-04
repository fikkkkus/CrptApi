import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import okhttp3.*;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CrptApiAllInOne {

    public interface CrptApiClient {
        UUID createIntroduceGoodsRF(RussianIntroduceDoc doc, String detachedSignatureBase64, String productGroup, String bearerToken) throws IOException, InterruptedException;
    }

    public static final class CrptConstants {
        public static final String BASE_URL = "https://ismp.crpt.ru";
        public static final String PATH_CREATE_DOC = "/api/v3/lk/documents/create";
        public static final String HEADER_ACCEPT = "*/*";
        public static final String HEADER_AUTH = "Authorization";
        public static final String CONTENT_TYPE_JSON = "application/json; charset=utf-8";
        public static final String DOC_TYPE_INTRODUCE_GOODS = "LP_INTRODUCE_GOODS";
        public static final String DOCUMENT_FORMAT = "MANUAL";
        public static final int DEFAULT_LIMIT = 5;
        public static final TimeUnit DEFAULT_UNIT = TimeUnit.SECONDS;
    }

    public static final class CrptApiConfig {
        public final String baseUrl;
        public final Duration timeout;
        public final int requestLimit;
        public final TimeUnit timeUnit;
        public final ObjectMapper objectMapper;
        public CrptApiConfig(String baseUrl, Duration timeout, int requestLimit, TimeUnit timeUnit) {
            this.baseUrl = Objects.requireNonNull(baseUrl);
            this.timeout = Objects.requireNonNull(timeout);
            this.requestLimit = requestLimit;
            this.timeUnit = Objects.requireNonNull(timeUnit);
            ObjectMapper om = new ObjectMapper();
            om.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
            this.objectMapper = om;
        }
    }

    static final class BlockingRateLimiter {
        private final long intervalNanos;
        private final int limit;
        private final Deque<Long> q = new ArrayDeque<>();
        BlockingRateLimiter(TimeUnit unit, int limit) {
            if (limit <= 0) throw new IllegalArgumentException("limit");
            this.intervalNanos = unit.toNanos(1);
            this.limit = limit;
        }
        synchronized void acquire() throws InterruptedException {
            for (;;) {
                long now = System.nanoTime();
                long cutoff = now - intervalNanos;
                while (!q.isEmpty() && q.peekFirst() < cutoff) q.pollFirst();
                if (q.size() < limit) {
                    q.addLast(now);
                    return;
                }
                long waitNanos = (q.peekFirst() + intervalNanos) - now;
                long ms = Math.max(1, TimeUnit.NANOSECONDS.toMillis(waitNanos));
                this.wait(ms);
            }
        }
    }

    public static final class CrptApiException extends RuntimeException {
        public CrptApiException(String message) { super(message); }
    }

    public static final class CreateDocumentPayload {
        @JsonProperty("product_document")
        public String productDocument;
        @JsonProperty("document_format")
        public String documentFormat;
        @JsonProperty("type")
        public String type;
        @JsonProperty("signature")
        public String signature;
    }

    public static final class ValueResponse {
        @JsonProperty("value")
        public String value;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class RussianIntroduceDoc {
        @JsonProperty("description")
        public Description description;
        @JsonProperty("doc_id")
        public String docId;
        @JsonProperty("doc_status")
        public String docStatus;
        @JsonProperty("doc_type")
        public String docType;
        @JsonProperty("importRequest")
        public Boolean importRequest;
        @JsonProperty("owner_inn")
        public String ownerInn;
        @JsonProperty("participant_inn")
        public String participantInn;
        @JsonProperty("producer_inn")
        public String producerInn;
        @JsonProperty("production_date")
        public String productionDate;
        @JsonProperty("production_type")
        public String productionType;
        @JsonProperty("products")
        public List<Product> products;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static final class Description {
            @JsonProperty("participantInn")
            public String participantInn;
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static final class Product {
            @JsonProperty("certificate_document")
            public String certificateDocument;
            @JsonProperty("certificate_document_date")
            public String certificateDocumentDate;
            @JsonProperty("certificate_document_number")
            public String certificateDocumentNumber;
            @JsonProperty("owner_inn")
            public String ownerInn;
            @JsonProperty("producer_inn")
            public String producerInn;
            @JsonProperty("production_date")
            public String productionDate;
            @JsonProperty("tnved_code")
            public String tnvedCode;
            @JsonProperty("uit_code")
            public String uitCode;
            @JsonProperty("uitu_code")
            public String uituCode;
        }

        public static Builder builder() { return new Builder(); }

        public static final class Builder {
            private final RussianIntroduceDoc d = new RussianIntroduceDoc();
            private final ArrayList<Product> list = new ArrayList<>();
            public Builder descriptionParticipantInn(String v) { Description desc = new Description(); desc.participantInn = v; d.description = desc; return this; }
            public Builder docId(String v) { d.docId = v; return this; }
            public Builder docStatus(String v) { d.docStatus = v; return this; }
            public Builder docType(String v) { d.docType = v; return this; }
            public Builder importRequest(Boolean v) { d.importRequest = v; return this; }
            public Builder ownerInn(String v) { d.ownerInn = v; return this; }
            public Builder participantInn(String v) { d.participantInn = v; return this; }
            public Builder producerInn(String v) { d.producerInn = v; return this; }
            public Builder productionDate(String v) { d.productionDate = v; return this; }
            public Builder productionType(String v) { d.productionType = v; return this; }
            public Builder addProduct(Product p) { list.add(Objects.requireNonNull(p)); return this; }
            public RussianIntroduceDoc build() { d.products = list; if (d.docType == null) d.docType = CrptConstants.DOC_TYPE_INTRODUCE_GOODS; return d; }
        }
    }

    public static final class CrptApiOkHttp implements CrptApiClient {
        private final OkHttpClient http;
        private final BlockingRateLimiter limiter;
        private final String baseUrl;
        private final ObjectMapper om;
        public CrptApiOkHttp(CrptApiConfig cfg) {
            this.http = new OkHttpClient.Builder()
                    .callTimeout(cfg.timeout)
                    .connectTimeout(cfg.timeout)
                    .readTimeout(cfg.timeout)
                    .writeTimeout(cfg.timeout)
                    .build();
            this.limiter = new BlockingRateLimiter(cfg.timeUnit, cfg.requestLimit);
            this.baseUrl = trimRight(cfg.baseUrl);
            this.om = cfg.objectMapper;
        }
        @Override
        public UUID createIntroduceGoodsRF(RussianIntroduceDoc doc, String detachedSignatureBase64, String productGroup, String bearerToken) throws IOException, InterruptedException {
            limiter.acquire();
            String endpoint = baseUrl + CrptConstants.PATH_CREATE_DOC + "?pg=" + urlEnc(productGroup);
            String productDocumentJson = om.writeValueAsString(doc);
            String productDocumentB64 = Base64.getEncoder().encodeToString(productDocumentJson.getBytes(StandardCharsets.UTF_8));
            CreateDocumentPayload payload = new CreateDocumentPayload();
            payload.productDocument = productDocumentB64;
            payload.documentFormat = CrptConstants.DOCUMENT_FORMAT;
            payload.type = CrptConstants.DOC_TYPE_INTRODUCE_GOODS;
            payload.signature = detachedSignatureBase64;
            String body = om.writeValueAsString(payload);
            Request req = new Request.Builder()
                    .url(endpoint)
                    .header("Accept", CrptConstants.HEADER_ACCEPT)
                    .header(CrptConstants.HEADER_AUTH, "Bearer " + bearerToken)
                    .post(RequestBody.create(body, MediaType.parse(CrptConstants.CONTENT_TYPE_JSON)))
                    .build();
            try (Response resp = http.newCall(req).execute()) {
                if (!resp.isSuccessful()) throw new CrptApiException("HTTP " + resp.code());
                String respBody = resp.body() != null ? resp.body().string() : "";
                ValueResponse vr = om.readValue(respBody, ValueResponse.class);
                if (vr == null || vr.value == null || vr.value.isEmpty()) throw new CrptApiException("No value");
                return UUID.fromString(vr.value);
            }
        }
        private static String urlEnc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }
        private static String trimRight(String u) { return u.endsWith("/") ? u.substring(0, u.length() - 1) : u; }
    }

    public static void main(String[] args) throws Exception {
        CrptApiConfig cfg = new CrptApiConfig(CrptConstants.BASE_URL, Duration.ofSeconds(30), CrptConstants.DEFAULT_LIMIT, CrptConstants.DEFAULT_UNIT);
        CrptApiClient api = new CrptApiOkHttp(cfg);
        RussianIntroduceDoc.Product p = new RussianIntroduceDoc.Product();
        p.ownerInn = "7701234567";
        p.producerInn = "7701234567";
        p.productionDate = "2025-11-04";
        p.tnvedCode = "6401100000";
        p.uitCode = "01XXXXXXXXXXXXXX";
        RussianIntroduceDoc doc = RussianIntroduceDoc.builder()
                .descriptionParticipantInn("7701234567")
                .docId(UUID.randomUUID().toString())
                .docStatus("NEW")
                .ownerInn("7701234567")
                .participantInn("7701234567")
                .producerInn("7701234567")
                .productionDate("2025-11-04")
                .productionType("AUTO")
                .addProduct(p)
                .build();
        UUID id = api.createIntroduceGoodsRF(doc, "<detached-signature-base64>", "milk", "<bearer-token>");
        System.out.println(id);
    }
}
