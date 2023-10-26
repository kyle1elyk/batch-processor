package dev.k1k.batchprocessor.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.k1k.batchprocessor.model.InputRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class InputObjJsonLineMapper implements LineMapper<InputRecord> {

    private final ObjectMapper objectMapper;


    /**
     * Interpret the line as a Json object and create a InputObj Entity from it.
     *
     * @see LineMapper#mapLine(String, int)
     */
    @Override
    public InputRecord mapLine(String line, int lineNumber) throws Exception {
        return objectMapper.readValue(line, InputRecord.class);
    }

}