package dev.k1k.batchprocessor.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.k1k.batchprocessor.model.InputRecord;
import dev.k1k.batchprocessor.model.OutputRecord;
import dev.k1k.batchprocessor.model.RecordMetaData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

import static software.amazon.awssdk.utils.StringUtils.isNotBlank;

@Component
@RequiredArgsConstructor
@Slf4j
public class InputRecordProcessor implements ItemProcessor<InputRecord, OutputRecord> {
    private final ObjectMapper objectMapper;
    @Override
    public OutputRecord process(InputRecord input) throws Exception {

        String inputToWrite = input.getInput();
        if (isNotBlank(input.getRecordMetaData())) {
            final RecordMetaData recordMetaData = objectMapper.readValue(input.getRecordMetaData(), RecordMetaData.class);
            inputToWrite += STR."_\{recordMetaData.getValue()}";
        }

        log.info(STR."\{ Instant.now().toString() }: PROCESSED \{inputToWrite}");
        return new OutputRecord(UUID.randomUUID(), inputToWrite, inputToWrite.hashCode());
    }
}
