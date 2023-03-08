package kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Properties;

public class MyKafkaProducer {
    
    private static final Logger log = LoggerFactory.getLogger(MyKafkaProducer.class);

    private static final String IP_ADDRESS = "192.168.244.128";
    private static final String BOOTSTRAP_BROKERS = IP_ADDRESS + ":9092";
    
    private static final String KAFKA_TOPIC = "my-topic";
    private static final String CLIENT_ID = "my-client";
    private static final String INSTRUMENTATION_NAME = "my-producer";
    private static final String SERVICE_NAME = "my-service";
    private static final String OTLP_ENDPOINT = "grpc://" + IP_ADDRESS + ":4317";

    //private static final String JAEGER_HOST = IP_ADDRESS;
    //private static final int JAEGER_PORT = 14250;
    
    //private static final String JAEGER_ENDPOINT = "http://" + JAEGER_HOST + ":" + JAEGER_PORT;
    

    public static void main(String[] args) throws Exception {

        System.out.println("BOOTSTRAP_BROKERS = " + BOOTSTRAP_BROKERS);
        System.out.println("Setting up kafka producer.");
        
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_BROKERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        System.out.println("Creating record.");
        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("my-topic", "hello world");

        Resource resource = Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "logical-service-name")));
        
        System.out.println("OTLP ENDPOINT = " + OTLP_ENDPOINT);
        
        OtlpGrpcSpanExporter otlp_grpc_span_exporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(OTLP_ENDPOINT)
                .build();
 
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(otlp_grpc_span_exporter).build())
            .setResource(resource)
            .build();

        OtlpGrpcMetricExporter otlp_grpc_metric_exporter = OtlpGrpcMetricExporter.builder()
            .setEndpoint(OTLP_ENDPOINT)
            .build();

        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.builder(otlp_grpc_metric_exporter).build())
            .setResource(resource)
        .   build();
        
        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setMeterProvider(sdkMeterProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();
              
        Tracer tracer =
            openTelemetry.getTracer("instrumentation-library-name", "1.0.0");        

        Span span = tracer.spanBuilder("my span").startSpan();

        // Make the span the current span
        try (Scope ss = span.makeCurrent()) {
            // In this scope, the span is the current/active span
            System.out.println("Sending record.");
            // send data - asynchronous
            producer.send(producerRecord);
            System.out.println("Done sending record.");
            // flush data - synchronous
            producer.flush();
            // flush and close producer
            producer.close();
        } catch (Throwable t) {
            span.setStatus(StatusCode.ERROR, "Something bad happened!");
            throw t;
        } finally {
            span.end();
        }              

    }
}
