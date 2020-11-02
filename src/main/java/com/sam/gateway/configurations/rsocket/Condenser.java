package com.sam.gateway.configurations.rsocket;

import com.sam.gateway.entities.BigRequest;
import com.sam.gateway.entities.MonoContainer;
import io.rsocket.SocketAcceptor;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

  private final UsernamePasswordMetadata credentials = new UsernamePasswordMetadata("jlong", "pw");
  private final MimeType mimeType =
      MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());
  LinkedList<MonoContainer> queue = new LinkedList<>();

  @Value("${core.RSocket.host:localhost}")
  private String coreRSocketHost;

  @Value("${core.RSocket.port:8888}")
  private Integer coreRSocketPort;

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

  private UnicastProcessor<BigRequest> data;

  @Autowired
  private SocketAcceptor acceptor;

  public Condenser(RSocketRequester.Builder builder) {

    this.rSocketBuilder = builder;
    this.shutDown.scheduleAtFixedRate(this.checkServerPing(), 1000, 1000, TimeUnit.MILLISECONDS);
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

  public void retryConnAndAlive() {
    System.out.println("connecting process");

      this.queue.stream().forEach(monoContainer -> {
          monoContainer.getMonoSink().error(new Exception("could not process!"));
      });
      this.queue.clear();

    if (this.client != null) {
      this.client.rsocket().dispose();
      this.client = null;
    }
    if (connection != null) {
      connection.dispose();
      connection = null;
    }
    if (amAliving != null) {
      amAliving.dispose();
      amAliving = null;
    }

    getRSocketRequester();
    startPing();
    connect();
    connected = true;
  }

  private void startPing(){
    client
            .route("startPing")
            .metadata(this.credentials, this.mimeType)
            .data(Mono.empty())
            .retrieveFlux(String.class)
            .doFirst(()->{
              System.out.println("hagase la luz");
            })
            .doOnNext(ping ->{
              System.out.println(ping);
            })
            .subscribe();

  }

  private void connect() {
    if (this.data != null) {
      data.sink().complete();
      data = null;
    }
    data = UnicastProcessor.create();
    this.sink = data.sink();

    connection =
        this.client
            .route("channel")
            .metadata(this.credentials, this.mimeType)
            // .data(Mono.empty())
            .data(data)
            .retrieveFlux(BigRequest.class)
            .retryWhen(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
            .doOnError(
                error -> {
                  System.out.println("Error sending data: " + error);
                })
            .doOnNext(
                bigRequest -> {
                  queue.pop().getMonoSink().success(bigRequest);
                  // System.out.println("ID: " + bigRequest.getId());
                })
            .subscribe();
  }

  private void getRSocketRequester() {
    this.client =
        this.rSocketBuilder
            .setupMetadata(this.credentials, this.mimeType)
            // .rsocketConnector(connector -> connector.acceptor(acceptor))
            .rsocketConnector(
                connector -> {
                  connector.acceptor(acceptor);
                  connector.payloadDecoder(PayloadDecoder.ZERO_COPY);
                  // connector.reconnect(Retry.fixedDelay(Integer.MAX_VALUE,
                  // Duration.ofSeconds(1)));

                })
            // .reconnect(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(5)))
            .connectTcp(coreRSocketHost, coreRSocketPort)
            .doOnSuccess(
                success -> {
                  System.out.println("Socket Connected!");
                })
            .doOnError(
                error -> {
                  System.out.println(error);
                })
            .retryWhen(
                Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1))
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
    synchronized (this) {
      this.queue.add(monoContainer);
      mySink.next(bigRequest);
    }

    return brMono;
  }

  public Runnable checkServerPing() {
    return () -> {
      // System.out.println("QUEUE SIZE = " + this.queue.size());
      /*if (connected && pinging) {
        Long now = System.currentTimeMillis();
        Long diff = now - pingTime;

        if (diff > 1600) {
          System.out.println(diff + " too long diff, reconnecting!");
          connected = false;
          pinging = false;
          startingPingTimes = 0;
        }
      }*/

      if (!connected) {
        synchronized (this) {
          if (!connected) {
            System.out.println("connecting process");
            if (this.client != null) {
              this.client.rsocket().dispose();
              this.client = null;
            }
            if (connection != null) {
              connection.dispose();
              connection = null;
            }
            if (amAliving != null) {
              amAliving.dispose();
              amAliving = null;
            }

            getRSocketRequester();
            startPing();
            connect();
            connected = true;
            // startAmAlive();
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

  @MessageMapping("health")
  Flux<ClientHealthState> health(Flux<String> ping) {
    ping.doOnNext(
            p -> {
              System.out.println(p);
            })
            .subscribe();
    var stream = Stream.generate(() -> new ClientHealthState(true));
    return Flux.fromStream(stream).delayElements(Duration.ofSeconds(1));
  }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class ClientHealthState {
  private boolean healthy;
}
