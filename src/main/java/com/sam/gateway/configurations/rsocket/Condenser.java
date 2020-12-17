package com.sam.gateway.configurations.rsocket;

import com.sam.commons.entities.BigRequest;
import com.sam.commons.entities.MenuItemReq;
import com.sam.gateway.entities.MonoContainer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.Disposable;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class Condenser {

  private final UsernamePasswordMetadata credentials = new UsernamePasswordMetadata("jlong", "pw");
  private final MimeType mimeType =
      MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());
  private LinkedList<MonoContainer<BigRequest>> queue = new LinkedList<>();
  private LinkedList<MonoContainer<MenuItemReq>> menuItemQueue = new LinkedList<>();
  private LinkedList<MonoContainer<MenuItemReq>> deleteMenuItemQueue = new LinkedList<>();

  @Value("${core.RSocket.host:localhost}")
  private String coreRSocketHost;

  @Value("${core.RSocket.port:8888}")
  private Integer coreRSocketPort;

  private RSocketRequester client;
  private RSocketRequester.Builder rSocketBuilder;
  private ExecutorService exec = Executors.newFixedThreadPool(4);
  private FluxSink<BigRequest> sink;
  private FluxSink<MenuItemReq> menuItemSink;
  private FluxSink<MenuItemReq> deleteMenuItemSink;
  private boolean connected = false;
  private boolean connecting = false;
  private ScheduledExecutorService shutDown = Executors.newSingleThreadScheduledExecutor();
  private Long pingTime = 0L;
  private boolean pinging = false;
  private int startingPingTimes = 0;
  private Disposable connection;
  private Disposable menuItemConnection;
  private Disposable deleteMenuItemConnection;
  private Disposable pingSubscription;
  private Disposable amAliving;
  private int channelConnErrTimes = 0;
  private int aliveConnErrTimes = 0;

  private UnicastProcessor<BigRequest> data;
  private UnicastProcessor<MenuItemReq> menuItemData;
  private UnicastProcessor<MenuItemReq> deleteMenuItemData;

  // @Autowired private SocketAcceptor acceptor;

  public Condenser(RSocketRequester.Builder builder) {

    this.rSocketBuilder = builder;
    this.shutDown.scheduleAtFixedRate(this.checkServerPing(), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  private FluxSink<BigRequest> getSink() {

    return this.sink;
  }

  public void disconnect(){
    this.pingSubscription.dispose();
    this.client.rsocket().dispose();
  }

  public void retryConnAndAlive() {
    System.out.println("connecting process");

    this.queue.stream()
        .forEach(
            monoContainer -> {
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

    if (pingSubscription != null) {
      pingSubscription.dispose();
      pingSubscription = null;
    }

    getRSocketRequester();
    startPing();
  }

  private void startPing() {
    pingSubscription =
        client
            .route("startPing")
            .metadata(this.credentials, this.mimeType)
            .data(Mono.empty())
            .retrieveFlux(String.class)
            .doOnError(
                error -> {
                  error.printStackTrace();
                })
            .doOnNext(
                ping -> {
                  if (!connected) {
                    System.out.println("pinging now connecting");
                    connect();
                    menuItemConnect();
                    deleteMenuItemConnect();
                    connected = true;
                    connecting = false;
                  }
                  pingTime = System.currentTimeMillis();
                })
            .subscribe();
  }

  private void deleteMenuItemConnect() {
    if (this.deleteMenuItemData != null) {
      this.deleteMenuItemData.sink().complete();
      this.deleteMenuItemData = null;
    }
    this.deleteMenuItemData = UnicastProcessor.create();
    this.deleteMenuItemSink = this.deleteMenuItemData.sink();

    deleteMenuItemConnection =
        this.client
            .route("deleteMenuItemReqChannel")
            .metadata(this.credentials, this.mimeType)
            // .data(Mono.empty())
            .data(deleteMenuItemData)
            .retrieveFlux(MenuItemReq.class)
            .retryWhen(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
            .doOnError(
                error -> {
                  System.out.println("Error sending data: " + error);
                })
            .doOnNext(
                menuItemReq -> {
                  deleteMenuItemQueue.pop().getMonoSink().success(menuItemReq);
                  // System.out.println("ID: " + bigRequest.getId());
                })
            .subscribe();
  }

  private void menuItemConnect() {
    if (this.menuItemData != null) {
      this.menuItemData.sink().complete();
      this.menuItemData = null;
    }
    this.menuItemData = UnicastProcessor.create();
    this.menuItemSink = this.menuItemData.sink();

    menuItemConnection =
        this.client
            .route("menuItemReqChannel")
            .metadata(this.credentials, this.mimeType)
            // .data(Mono.empty())
            .data(menuItemData)
            .retrieveFlux(MenuItemReq.class)
            .retryWhen(Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(1)))
            .doOnError(
                error -> {
                  System.out.println("Error sending data: " + error);
                })
            .doOnNext(
                menuItemReq -> {
                  System.out.println("esto es nulo? " + menuItemReq);
                  menuItemQueue.pop().getMonoSink().success(menuItemReq);
                  // System.out.println("ID: " + bigRequest.getId());
                })
            .subscribe();
  }

  private void connect() {
    if (this.data != null) {
      data.sink().complete();
      data = null;
    }
    this.data = UnicastProcessor.create();
    this.sink = this.data.sink();

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
                  // connector.acceptor(acceptor);
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

  public Mono<MenuItemReq> doCondenseDeleteMenuItems(MenuItemReq menuItemReq) {
    if (!connected) {
      throw new RuntimeException("NOT CONNECTED");
    }

    FluxSink<MenuItemReq> mySink = this.deleteMenuItemSink;

    MonoContainer<MenuItemReq> monoContainer = new MonoContainer();
    Mono<MenuItemReq> menuItemMono =
        Mono.create(
            s -> {
              monoContainer.setMonoSink(s);
            });

    // Mono.create(s -> s.onCancel(() -> cancelled.set(true)).success("test"))
    synchronized (this) {
      this.deleteMenuItemQueue.add(monoContainer);
      mySink.next(menuItemReq);
    }

    return menuItemMono;
  }

  public Mono<MenuItemReq> doCondenseMenuItems(MenuItemReq menuItemReq) {
    if (!connected) {
      throw new RuntimeException("NOT CONNECTED");
    }

    FluxSink<MenuItemReq> mySink = this.menuItemSink;

    MonoContainer<MenuItemReq> monoContainer = new MonoContainer();
    Mono<MenuItemReq> menuItemMono =
        Mono.create(
            s -> {
              monoContainer.setMonoSink(s);
            });

    // Mono.create(s -> s.onCancel(() -> cancelled.set(true)).success("test"))
    synchronized (this) {
      this.menuItemQueue.add(monoContainer);
      mySink.next(menuItemReq);
    }

    return menuItemMono;
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
      if (connected) {
        Long now = System.currentTimeMillis();
        Long diff = now - pingTime;

        if (diff > 1200) {
          System.out.println(diff + " too long diff, reconnecting!");
          connected = false;
        }
      }

      if (!connected && !connecting) {
        connecting = true;
        System.out.println("connecting process");
        if (this.client != null) {
          this.client.rsocket().dispose();
          this.client = null;
        }
        if (connection != null) {
          connection.dispose();
          connection = null;
        }

        if (menuItemConnection != null) {
          menuItemConnection.dispose();
          menuItemConnection = null;
        }

        if (deleteMenuItemConnection != null) {
          deleteMenuItemConnection.dispose();
          deleteMenuItemConnection = null;
        }

        if (amAliving != null) {
          amAliving.dispose();
          amAliving = null;
        }

        if (pingSubscription != null) {
          pingSubscription.dispose();
          pingSubscription = null;
        }

        getRSocketRequester();
        startPing();
      }
    };
  }
}
