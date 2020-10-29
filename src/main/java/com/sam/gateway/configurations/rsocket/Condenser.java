package com.sam.gateway.configurations.rsocket;

import com.sam.gateway.entities.BigRequest;
import com.sam.gateway.entities.MonoContainer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.Disposable;
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

  @Value("${core.rSocket.host:localhost}")
  private String coreRSocketHost;

  @Value("${core.rSocket.port:localhost}")
  private Integer coreRSocketPort;

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
  private Long pingTime = 0L;
  private boolean pinging = false;
  private int startingPingTimes = 0;
  private Disposable connection;
  private Disposable amAliving;
  private int channelConnErrTimes = 0;
  private int aliveConnErrTimes = 0;

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

  public void retryConnAndAlive(){
    connected = false;

  }

  private void startAmAlive() {
    Flux<String> ping =
        Flux.fromStream(Stream.generate(() -> "ping")).delayElements(Duration.ofMillis(1500));
    amAliving =
        this.client
            .route("startAmAlive")
            .metadata(this.credentials, this.mimeType)
            .data(ping)
            .retrieveFlux(String.class)
            .retryWhen(Retry.indefinitely())
            .doOnError(
                error -> {
                  System.out.println(error);
                })
            .doOnNext(
                pong -> {
                  if (!pinging) {
                    if (startingPingTimes < 3) {
                      startingPingTimes++;
                    } else {
                      pinging = true;
                      pingTime = System.currentTimeMillis();
                    }
                  }
                  pingTime = System.currentTimeMillis();
                  //System.out.println("alive " + pingTime);
                })
            .subscribe();
  }

  private void connect() {
    UnicastProcessor<BigRequest> data = UnicastProcessor.create();
    this.sink = data.sink();

    connection =
        this.client
            .route("channel")
            .metadata(this.credentials, this.mimeType)
            // .data(Mono.empty())
            .data(data)
            .retrieveFlux(BigRequest.class)
            .retryWhen(Retry.indefinitely())
            .doOnError(
                error -> {
                  System.out.println(error);
                })
            .doOnNext(
                bigRequest -> {
                  queue.pop().getMonoSink().success(bigRequest);
                  //System.out.println("ID: " + bigRequest.getId());
                })
            .subscribe();
  }

  private void getRSocketRequester() {
    this.client =
        this.rSocketBuilder
            .setupMetadata(this.credentials, this.mimeType)
            // .rsocketConnector(connector -> connector.acceptor(acceptor))
            .rsocketConnector(connector -> connector.payloadDecoder(PayloadDecoder.ZERO_COPY))
            // .reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(5)))
            .connectTcp(coreRSocketHost, coreRSocketPort)
            .doOnSuccess(
                success -> {
                  System.out.println("Socket Connected!");
                })
            .retryWhen(
                Retry.indefinitely()
                    .doAfterRetry(
                        signal -> {
                          // log.info("Retrying times:  " + signal.totalRetriesInARow());
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
    synchronized (this){
      this.queue.add(monoContainer);
      mySink.next(bigRequest);
    }

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
      //System.out.println("QUEUE SIZE = " + this.queue.size());
      if (connected && pinging) {
        Long now = System.currentTimeMillis();
        Long diff = now - pingTime;

        if (diff > 1600) {
          System.out.println(diff + " too long diff, reconnecting!");
          connected = false;
          pinging = false;
          startingPingTimes = 0;
        }
      }

      if (!connected) {
        synchronized (this) {
          if (!connected) {
            System.out.println("connecting process");
            if (this.client != null){
              this.client.rsocket().dispose();
              this.client = null;
            }
            if (connection != null){
              connection.dispose();
              connection = null;
            }
            if (amAliving != null){
              amAliving.dispose();
              amAliving = null;
            }
            getRSocketRequester();
            connect();
            connected = true;
            startAmAlive();
            pingTime = System.currentTimeMillis();
          }
        }
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
