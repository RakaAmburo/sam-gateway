package com.sam.gateway.controllers;

import com.sam.gateway.configurations.rsocket.Condenser;
import com.sam.gateway.entities.BigRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/test")
public class Test {

  private Condenser condenser;

  @Autowired
  public Test(Condenser condenser) {
    this.condenser = condenser;
  }

 /* @GetMapping()
  Flux<String> getAll() {

    // condenser.test();

    return Flux.just("Its working");
  }*/

  @PostMapping("/condense")
  public  Mono<BigRequest> add(@RequestBody BigRequest bigRequest) {
    Mono<BigRequest> resp = condenser.doCondense(bigRequest);

    return resp;
  }

  @GetMapping("/stop")
  public Mono<Void> stop(){
    System.exit(1);
    return Mono.empty();
  }

  @GetMapping("/retry")
  public Mono<Void> retry(){
    condenser.retryConnAndAlive();
    return Mono.empty();
  }
}
