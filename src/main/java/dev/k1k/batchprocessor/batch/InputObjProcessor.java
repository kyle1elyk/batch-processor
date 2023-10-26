package dev.k1k.batchprocessor.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class InputObjProcessor implements ItemProcessor<InputObjProcessor.InputObj, InputObjProcessor.OutputObj> {


    @Override
    public OutputObj process(InputObj input) throws Exception {

        return new OutputObj(input.getInput(), input.getInput().hashCode());
    }

    @Data @AllArgsConstructor @NoArgsConstructor
    public static class InputObj {
        String input;
    }

    @Data @AllArgsConstructor @NoArgsConstructor
    public static class OutputObj {
        String input;
        int hash;
    }
}
