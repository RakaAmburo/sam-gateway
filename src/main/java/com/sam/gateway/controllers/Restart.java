package com.sam.gateway.controllers;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.UUID;

@RestController
public class Restart {

  @GetMapping("/restart")
  public String retrieveAllStudents() throws FileNotFoundException {
    File file = new File("src/main/resources/restart");
    PrintWriter writer = new PrintWriter(file.getAbsolutePath());
    writer.print(UUID.randomUUID().toString());
    writer.close();
    file.setLastModified(System.currentTimeMillis());
    return "Restarting";
  }
}
