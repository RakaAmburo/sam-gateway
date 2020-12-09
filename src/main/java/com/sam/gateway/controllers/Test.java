package com.sam.gateway.controllers;

import com.sam.commons.entities.Action;
import com.sam.commons.entities.BigRequest;
import com.sam.commons.entities.MenuItemDTO;
import com.sam.commons.entities.MenuItemReq;
import com.sam.commons.entities.Status;
import com.sam.gateway.configurations.rsocket.Condenser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

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
  public Mono<BigRequest> add(@RequestBody BigRequest bigRequest) {
    Mono<BigRequest> resp = condenser.doCondense(bigRequest);

    return resp;
  }

  @PostMapping("/addMenuItem")
  public Mono<MenuItemDTO> addMenuItem(@RequestBody MenuItemDTO menuItemDTO) {
    MenuItemReq menuItemReq = new MenuItemReq();
    menuItemReq.setId(UUID.randomUUID());
    menuItemReq.setAction(Action.INSERT);
    menuItemReq.setMenuItemDTO(menuItemDTO);
    menuItemReq.setStatus(Status.OK);
    Mono<MenuItemReq> resp = condenser.doCondenseMenuItems(menuItemReq);
    return resp.map(item -> item.getMenuItemDTO());
  }

  @DeleteMapping("/deleteMenuItem")
  public Mono<ResponseEntity<MenuItemDTO>> deleteMenuItem(@RequestBody MenuItemDTO menuItemDTO) {
    MenuItemReq menuItemReq = new MenuItemReq();
    menuItemReq.setId(UUID.randomUUID());
    menuItemReq.setAction(Action.DELETE);
    menuItemReq.setMenuItemDTO(menuItemDTO);
    menuItemReq.setStatus(Status.OK);
    Mono<MenuItemReq> resp = condenser.doCondenseDeleteMenuItems(menuItemReq);

    return Mono.from(resp).map( response -> {
      if (response.getStatus() == Status.ERROR){
        return ResponseEntity.status(HttpStatus.CONFLICT).body(response.getMenuItemDTO());
      } else {
        return ResponseEntity.ok(response.getMenuItemDTO());
      }
    });
  }

  @GetMapping("/stop")
  public Mono<Void> stop() {
    System.exit(1);
    return Mono.empty();
  }

  @GetMapping("/retry")
  public Mono<Void> retry() {
    condenser.retryConnAndAlive();
    return Mono.empty();
  }
}
