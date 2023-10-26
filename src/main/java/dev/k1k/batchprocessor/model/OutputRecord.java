package dev.k1k.batchprocessor.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OutputRecord {
    UUID recordId;
    String input;
    int hash;
}
