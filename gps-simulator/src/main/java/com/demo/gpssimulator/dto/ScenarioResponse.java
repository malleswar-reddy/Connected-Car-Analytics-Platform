package com.demo.gpssimulator.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioResponse {
    private String carId;
    private String scenario;
    /** The forced speed values (km/h) queued up, in the order they'll be published. */
    private List<Double> queuedSpeedsKmh;
}
