package com.sam.gateway.entities;

import com.sam.commons.entities.BigRequest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.MonoSink;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MonoContainer {
    MonoSink<BigRequest> monoSink;
}
