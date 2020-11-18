package com.sam.gateway.controllers;

import com.sam.commons.entities.BigRequest;
import com.sam.gateway.configurations.rsocket.Condenser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

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
