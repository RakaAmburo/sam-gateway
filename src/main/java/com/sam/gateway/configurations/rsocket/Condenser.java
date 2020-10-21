package com.sam.gateway.configurations.rsocket;

import com.sam.gateway.entities.BigRequest;
import com.sam.gateway.entities.MonoContainer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@Component
public class Condenser {

  private final UsernamePasswordMetadata credentials = new UsernamePasswordMetadata("jlong", "pw");
  private final MimeType mimeType =
      MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());
  LinkedList<MonoContainer> queue = new LinkedList<>();
  private RSocketRequester client;
  private RSocketRequester.Builder rSocketBuilder;
  private ExecutorService exec = Executors.newFixedThreadPool(4);
  private FluxSink<BigRequest> sink;
  private boolean connected = false;
  private ScheduledExecutorService shutDown = Executors.newSingleThreadScheduledExecutor();
  private Long pingTime;

  public Condenser(RSocketRequester.Builder builder) {

    this.rSocketBuilder = builder;
    this.shutDown.scheduleAtFixedRate(this.checkServerPing(), 2, 1500, TimeUnit.MILLISECONDS);
  }

  private FluxSink<BigRequest> getSink() {

    /*if (!connected) {
      getRSocketRequester();
      connect();
      connected = true;
      startAmAlive();
    }*/
    return this.sink;
  }

  private void startAmAlive() {
    Flux<String> ping =
        Flux.fromStream(Stream.generate(() -> "ping")).delayElements(Duration.ofMillis(1500));
    this.client
        .route("startAmAlive")
        .metadata(this.credentials, this.mimeType)
        .data(ping)
        .retrieveFlux(String.class)
        .doOnNext(
            pong -> {
              pingTime = System.currentTimeMillis();
              // System.out.println(pingTime);
            })
        .subscribe();
  }

  private void connect() {
    UnicastProcessor<BigRequest> data = UnicastProcessor.create();
    this.sink = data.sink();

    this.client
        .route("channel")
        .metadata(this.credentials, this.mimeType)
        // .data(Mono.empty())
        .data(data)
        .retrieveFlux(BigRequest.class)
        .doOnNext(
            bigRequest -> {
              queue.pop().getMonoSink().success(bigRequest);
              System.out.println("ID: " + bigRequest.getId());
            })
        .subscribe();
  }

  private void getRSocketRequester() {
    this.client =
        this.rSocketBuilder
            .setupMetadata(this.credentials, this.mimeType)
            // .rsocketConnector(connector -> connector.acceptor(acceptor))
            .rsocketConnector(connector -> connector.payloadDecoder(PayloadDecoder.ZERO_COPY))
                //.reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(5)))
            .connectTcp("localhost", 8888)
            .doOnSuccess(
                success -> {
                  System.out.println("socket connected");
                })
            .retryWhen(
                Retry.indefinitely()
                    .doAfterRetry(
                        signal -> {
                          log.info("Retrying times:  " + signal.totalRetriesInARow());
                        }))
            .block();

  }

  public Mono<BigRequest> doCondense(BigRequest bigRequest) {
    if (!connected) {
      throw new RuntimeException("NOT CONNECTED");
    }
    FluxSink<BigRequest> mySink = getSink();

    MonoContainer monoContainer = new MonoContainer();
    Mono<BigRequest> brMono =
        Mono.create(
            s -> {
              monoContainer.setMonoSink(s);
            });

    // Mono.create(s -> s.onCancel(() -> cancelled.set(true)).success("test"))
    this.queue.add(monoContainer);
    mySink.next(bigRequest);
    return brMono;
  }

  public void test() {

    UnicastProcessor<BigRequest> data = UnicastProcessor.create();
    FluxSink<BigRequest> sink = data.sink();
    IntStream.range(0, 100)
        .forEach(
            i -> {
              exec.execute(dispatchCalls(sink));
            });

    client
        .route("channel")
        .metadata(this.credentials, this.mimeType)
        // .data(Mono.empty())
        .data(data)
        .retrieveFlux(BigRequest.class)
        .doOnNext(
            bigRequest -> {
              System.out.println("ID: " + bigRequest.getId());
            })
        .subscribe();
  }

  public Runnable checkServerPing() {
    return () -> {
      if (pingTime != null) {
        Long now = System.currentTimeMillis();
        Long diff = now - pingTime;
        if (diff > 1500) {
          connected = false;
        }
      }

      if (!connected) {
        getRSocketRequester();
        connect();
        connected = true;
        pingTime = null;
        startAmAlive();
      }


    };
  }

  public Runnable dispatchCalls(FluxSink<BigRequest> sink) {
    return () -> {
      BigRequest br =
          new BigRequest(UUID.randomUUID(), List.of(UUID.randomUUID(), UUID.randomUUID()));
      sink.next(br);
    };
  }
}

@Controller
class HealthController {

  @MessageMapping("amAlive")
  Flux<String> amAlive(Flux<String> ping) {
    System.out.println("entra al ping");
    ping.doOnNext(
            p -> {
              System.out.println(p);
            })
        .subscribe();
    return Flux.interval(Duration.ofSeconds(1)).map(p -> "pong");
  }
}
