package dev.k1k.batchprocessor.batch;

import dev.k1k.batchprocessor.model.InputRecord;
import dev.k1k.batchprocessor.model.OutputRecord;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.DefaultExecutionContextSerializer;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.support.DefaultDataFieldMaxValueIncrementerFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.support.DatabaseType;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.lang.NonNull;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Slf4j
@Configuration
@AllArgsConstructor
@EnableBatchProcessing
public class BatchConfig {
    @Bean
    public FlatFileItemReader<InputRecord> reader(final InputObjJsonLineMapper inputObjJsonLineMapper) {
        return new FlatFileItemReaderBuilder<InputRecord>()
                .name("inputObjReader")
                .resource(new ClassPathResource("input.jsonl"))
                .lineMapper(inputObjJsonLineMapper)
                /*.delimited()
                .names("input")
                .targetType(InputRecord.class)*/.build();
    }

    @Bean
    public FlatFileItemWriter<OutputRecord> writer() {
        return new FlatFileItemWriterBuilder<OutputRecord>()
                .name("output-record-Writer")
                .resource(new FileSystemResource("output/outputData.csv"))
                .append(true)
                .lineAggregator(new DelimitedLineAggregator<>() {{
                    setDelimiter(",");
                    setFieldExtractor(new BeanWrapperFieldExtractor<>(){{
                        setNames(new String[]{
                                "recordId",
                                "input",
                                "hash"
                        });
                    }});
                }})
                .build();
    }

    @Bean
    public Job job(final JobRepository jobRepository, final Step processRecordStep) {
        return new JobBuilder("process-record", jobRepository)
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(@NonNull final JobExecution jobExecution) {
                        log.info(STR."JOB \{jobExecution.getJobId()} STARTING");
                    }

                    @Override
                    public void afterJob(@NonNull final JobExecution jobExecution) {
                        log.info(STR."JOB \{jobExecution.getJobId()} COMPLETE, STATUS: \{jobExecution.getStatus()}");
                    }
                })
                .start(processRecordStep).build();
    }

    @Bean
    public Step processRecordStep(final JobRepository jobRepository,
                                  final PlatformTransactionManager transactionManager,
                                  final FlatFileItemReader<InputRecord> reader,
                                  final InputRecordProcessor inputRecordProcessor,
                                  final FlatFileItemWriter<OutputRecord> writer) {

        return new StepBuilder("process-record-step", jobRepository)
                .<InputRecord, OutputRecord>chunk(1000, transactionManager)
                .reader(reader)
                .processor(inputRecordProcessor)
                .writer(writer)
                .build();
    }
    @Bean
    JobLauncherApplicationRunner jobLauncherApplicationRunner(final JobLauncher jobLauncher, final JobExplorer jobExplorer, final JobRepository jobRepository) {
        return new JobLauncherApplicationRunner(jobLauncher, jobExplorer, jobRepository);
    }

    @Bean
    public DataSource dataSource() {
        EmbeddedDatabaseBuilder embeddedDatabaseBuilder = new EmbeddedDatabaseBuilder();
        return embeddedDatabaseBuilder.addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
                .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
                .setType(EmbeddedDatabaseType.H2)
                .build();
    }

    @Bean
    public ResourcelessTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public JobRepository jobRepository(final DataSource dataSource, final PlatformTransactionManager transactionManager, JdbcOperations jdbcOperations) throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDatabaseType(DatabaseType.H2.getProductName());
        factory.setDataSource(dataSource);
        factory.setTransactionManager(transactionManager);
        factory.setIncrementerFactory(new DefaultDataFieldMaxValueIncrementerFactory(dataSource));
        factory.setJdbcOperations(jdbcOperations);
        factory.setConversionService(new DefaultConversionService());
        factory.setSerializer(new DefaultExecutionContextSerializer());
        return factory.getObject();
    }
}
