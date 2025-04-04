package by.froleod.ws.emailnotification;

import by.froleod.ws.core.ProductCreatedEvent;
import by.froleod.ws.emailnotification.handler.ProductCreatedEventHandler;
import by.froleod.ws.emailnotification.persistance.entity.ProcessedEventEntity;
import by.froleod.ws.emailnotification.persistance.repo.ProcessedEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ActiveProfiles("test")
@EmbeddedKafka(partitions = 3, topics = { "product-created-events-topic" })
@SpringBootTest(properties = "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductCreatedEventHandlerIntegrationTest {

    @Mock
    ProcessedEventRepository processedEventRepository;

    @Mock
    RestTemplate restTemplate;

    @Autowired
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    @Spy
    ProductCreatedEventHandler productCreatedEventHandler;


    @Test
    void productCreatedEventHandler_onProductCreated_HandlesEvent() throws ExecutionException, InterruptedException {
        //Arrange
        ProductCreatedEvent event = new ProductCreatedEvent();
        event.setProductId(UUID.randomUUID().toString());
        event.setPrice(BigDecimal.valueOf(100));
        event.setQuantity(5);
        event.setTitle("Test product");

        String messageId = UUID.randomUUID().toString();
        String messageKey = event.getProductId();

        ProducerRecord<String, Object> record = new ProducerRecord<>("product-created-events-topic", messageKey, event);
        record.headers().add("messageId", messageId.getBytes());
        record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

        ProcessedEventEntity entity = new ProcessedEventEntity();
        when(processedEventRepository.findByMessageId(anyString())).thenReturn(entity);
        when(processedEventRepository.save(any(ProcessedEventEntity.class))).thenReturn(null);


        String responseBody = "{\"key\":\"value\"}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);

        when(restTemplate.exchange(
                any(String.class),
                any(HttpMethod.class),
                isNull(), ArgumentMatchers.eq(String.class)
        )).thenReturn(responseEntity);


        //Act
        kafkaTemplate.send((Message<?>) record).get();


        //Assets
        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);


        verify(productCreatedEventHandler, timeout(5000).times(1)).handle(eventCaptor.capture(),
                messageIdCaptor.capture(), messageKeyCaptor.capture());

        assertEquals(messageId, messageIdCaptor.getValue());
        assertEquals(messageKey, messageKeyCaptor.getValue());
        assertEquals(event.getProductId(), eventCaptor.getValue().getProductId());


    }


}
