package dev.k1k.batchprocessor.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.stereotype.Component;

@Component
public class InputObjJsonLineMapper implements LineMapper<InputObjProcessor.InputObj> {

    private ObjectMapper mapper = new ObjectMapper();


    /**
     * Interpret the line as a Json object and create a InputObj Entity from it.
     *
     * @see LineMapper#mapLine(String, int)
     */
    @Override
    public InputObjProcessor.InputObj mapLine(String line, int lineNumber) throws Exception {
        return mapper.readValue(line, InputObjProcessor.InputObj.class);
    }

}