package com.kasi.batchpoc.config;

import com.kasi.batchpoc.model.Asset;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.net.MalformedURLException;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfiguration {
    @Bean("CsvToH2_Job")
    public Job csvToH2Job(JobBuilderFactory jobBuilderFactory,
                   StepBuilderFactory stepBuilderFactory,
                   @Qualifier("CsvToH2_Reader") ItemReader<Asset> itemReader,
                   @Qualifier("CsvToH2_Processor") ItemProcessor<Asset, Asset> itemProcessor,
                   @Qualifier("CsvToH2_Writer") ItemWriter<Asset> itemWriter
    )
    {

        Step step = stepBuilderFactory.get("CsvToH2_Step")
                .<Asset, Asset>chunk(2000)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();


        return jobBuilderFactory.get("CsvToH2_Job")
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean("CsvToH2_Reader")
    public FlatFileItemReader<Asset> itemReader() {

        FlatFileItemReader<Asset> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new ClassPathResource("AssetData_10R.csv"));
        flatFileItemReader.setName("CSV-Reader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }

    @Bean
    public LineMapper<Asset> lineMapper() {

        DefaultLineMapper<Asset> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(new String[]{"assetId", "assetTypeCode"});

        BeanWrapperFieldSetMapper<Asset> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Asset.class);

        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);

        return defaultLineMapper;
    }

    @Bean("multiResourcePartitioner")
    @StepScope
    public Partitioner partitioner() {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = null;
        try {
            resources = resolver.getResources("parallel/*.csv");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        partitioner.setResources(resources);
        //partitioner.partition(10);
        return partitioner;
    }

    @Bean("CsvToH2_Parallel_Job")
    public Job csvToH2ParallelJob(JobBuilderFactory jobBuilderFactory,
                   @Qualifier("CsvToH2_Parallel_MasterStep") Step masterStep
    )
    {
        return jobBuilderFactory.get("CsvToH2_Parallel_Job")
                .incrementer(new RunIdIncrementer())
                .start(masterStep)
                .build();
    }

    @Bean
    @Qualifier("CsvToH2_Parallel_MasterStep")
    public Step masterStep(StepBuilderFactory stepBuilderFactory,
            @Qualifier("CsvToH2_Parallel_SlaveStep") Step slaveStep,
            @Qualifier("multiResourcePartitioner") Partitioner multiResourcePartitioner,
            @Qualifier("CsvToH2_Parallel_TaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        return stepBuilderFactory.get("CsvToH2_Parallel_MasterStep")
                .partitioner("CsvToH2_Parallel_SlaveStep", multiResourcePartitioner)
                .step(slaveStep)
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean("CsvToH2_Parallel_TaskExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(10);
        taskExecutor.setCorePoolSize(10);
        return taskExecutor;
    }

    @Bean("CsvToH2_Parallel_SlaveStep")
    public Step slaveStep(StepBuilderFactory stepBuilderFactory,
                          @Qualifier("CsvToH2_Parallel_Reader") ItemReader<Asset> itemReader,
                          @Qualifier("CsvToH2_Processor") ItemProcessor<Asset, Asset> itemProcessor,
                          @Qualifier("CsvToH2_Writer") ItemWriter<Asset> itemWriter) {
        return stepBuilderFactory.get("CsvToH2_Parallel_SlaveStep")
                .<Asset, Asset>chunk(2000)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();
    }

    @Bean
    @StepScope
    @Qualifier("CsvToH2_Parallel_Reader")
    @DependsOn("multiResourcePartitioner")
    public FlatFileItemReader<Asset> personItemReader(@Value("#{stepExecutionContext['fileName']}") String filename) throws MalformedURLException {

        FlatFileItemReader<Asset> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new UrlResource(filename));
        flatFileItemReader.setName("CsvToH2_Parallel_Reader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapper());
        return flatFileItemReader;
    }
}
