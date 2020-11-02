package com.sam.gateway.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;

@RestController
public class Restart {

  @GetMapping("/restart")
  public String restart() {
    File f = new File("restart");
    f.setLastModified(System.currentTimeMillis());
    return "Restarting";
  }
}
