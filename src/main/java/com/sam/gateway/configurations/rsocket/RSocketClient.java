package com.sam.gateway.configurations.rsocket;

import io.rsocket.SocketAcceptor;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import org.springframework.boot.rsocket.messaging.RSocketStrategiesCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.rsocket.metadata.SimpleAuthenticationEncoder;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.util.retry.Retry;

import java.time.Duration;

@Configuration
public class RSocketClient {

  private final UsernamePasswordMetadata credentials = new UsernamePasswordMetadata("jlong", "pw");
  private final MimeType mimeType =
      MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());

  @Bean
  RSocketStrategiesCustomizer strategiesCustomizer() {
    return strategies -> strategies.encoder(new SimpleAuthenticationEncoder());
  }

  /*@Bean
  SocketAcceptor socketAcceptor(RSocketStrategies strategies, HealthController controller) {
      return RSocketMessageHandler.responder(strategies, controller);
  }*/

  //@Bean
  // RSocketRequester rSocketRequester(SocketAcceptor acceptor, RSocketRequester.Builder builder) {

  /*RSocketRequester rSocketRequester(RSocketRequester.Builder builder) {
    return builder
        .setupMetadata(this.credentials, this.mimeType)
        // .rsocketConnector(connector -> connector.acceptor(acceptor))
        .rsocketConnector(
            connector ->
                connector
                    .payloadDecoder(PayloadDecoder.ZERO_COPY)
                    .reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(5))))
        .connectTcp("localhost", 8888)
        .retryWhen(
            Retry.indefinitely()
                .doAfterRetry(
                    signal -> {
                      System.out.println("Retrying times:  " + signal.totalRetriesInARow());
                    }))
        .block();
  }*/
}
