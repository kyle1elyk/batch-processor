package dev.k1k.batchprocessor.batch;

import lombok.AllArgsConstructor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
@AllArgsConstructor
public class BatchConfig {

    public final InputObjJsonLineMapper inputObjJsonLineMapper;
    public final InputObjProcessor inputObjProcessor;

    @Bean
    public FlatFileItemReader<InputObjProcessor.InputObj> reader() {
        return new FlatFileItemReaderBuilder<InputObjProcessor.InputObj>()
                .name("inputObjReader")
                .resource(new ClassPathResource("input.jsonl"))
                .lineMapper(inputObjJsonLineMapper)
                .delimited()
                .names("input")
                .targetType(InputObjProcessor.InputObj.class).build();
    }

    @Bean
    public FlatFileItemWriter<InputObjProcessor.OutputObj> writer() {
        return new FlatFileItemWriterBuilder<InputObjProcessor.OutputObj>()
                .name("outputObjWriter")
                .build();
    }
}
