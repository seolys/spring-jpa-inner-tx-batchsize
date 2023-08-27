package seol.study.springbatch.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import seol.study.springbatch.domain.Store;
import seol.study.springbatch.domain.StoreHistory;
import seol.study.springbatch.domain.StoreWriteService;

import javax.persistence.EntityManagerFactory;
import java.util.LinkedHashMap;
import java.util.Map;


@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = N1JpaPaging2JobConfig.JOB_NAME)
public class N1JpaPaging2JobConfig {

    public static final String JOB_NAME = "n1JpaPagingJob2";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final StoreWriteService storeWriteService;

    private int chunkSize;

    @Value("${chunkSize:100}")
    public void setChunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Bean(name = JOB_NAME)
    public Job job() {
        return jobBuilderFactory.get(JOB_NAME)
                .start(initData())
                .next(step())
                .build();
    }

    @Bean(name = JOB_NAME + "_initData")
    Step initData() {
        return stepBuilderFactory.get(JOB_NAME + "_initData")
                .tasklet((contribution, chunkContext) -> {
                    storeWriteService.saveTestData();
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean(name = JOB_NAME + "_step")
    @JobScope
    public Step step() {
        return stepBuilderFactory.get(JOB_NAME + "_step")
                .<Store, StoreHistory>chunk(chunkSize)
                .reader(reader(null))
                .processor(processor())
                .writer(writer())
                .allowStartIfComplete(true)
                .build();
    }

    // JpaPagingItemReader
    @Bean(name = JOB_NAME + "_reader")
    @StepScope
    public JpaPagingItemReader<Store> reader(
            @Value("#{jobParameters[address]}") final String address) {

        final Map<String, Object> parameters = new LinkedHashMap<>();
//        parameters.put("address", address + "%");
        parameters.put("address", "서울" + "%");

        return new JpaPagingItemReaderBuilder<Store>()
                .pageSize(chunkSize)
                .parameterValues(parameters)
                .queryString("SELECT s FROM Store s WHERE s.address LIKE :address order by s.id")
                .entityManagerFactory(entityManagerFactory)
                .name(JOB_NAME + "_reader")
                .transacted(true)
                .build();
    }

    // JpaPagingFetchItemReader
//    @Bean
//    @StepScope
//    public JpaPagingFetchItemReader<Store> reader(
//            @Value("#{jobParameters[address]}") final String address) {
//
//        final Map<String, Object> parameters = new LinkedHashMap<>();
//        parameters.put("address", address + "%");
//
//        final JpaPagingFetchItemReader<Store> reader = new JpaPagingFetchItemReader<>();
//        reader.setEntityManagerFactory(entityManagerFactory);
//        reader.setQueryString("SELECT s FROM Store s WHERE s.address LIKE :address order by s.id");
//        reader.setParameterValues(parameters);
//        reader.setPageSize(chunkSize);
//
//        return reader;
//    }

    // QuerydslPagingItemReader
//    @Bean
//    @StepScope
//    public QuerydslPagingItemReader<Store> reader(@Value("#{jobParameters[address]}") final String address) {
//        return new QuerydslPagingItemReader<>(entityManagerFactory, chunkSize, queryFactory -> {
//            // 요청 시간 기준으로 만료 기간이 지났지만, "적립" 포인트가 남아있는 경우 조회
//            return queryFactory
//                    .selectFrom(QStore.store)
//                    .where(QStore.store.address.like(address + "%"));
//        });
//    }

    // QuerydslCursorItemReader
//    @Bean
//    @StepScope
//    public QuerydslCursorItemReader<Store> reader(@Value("#{jobParameters[address]}") final String address) {
//        return new QuerydslCursorItemReader<>(entityManagerFactory, chunkSize, queryFactory -> {
//            // 요청 시간 기준으로 만료 기간이 지났지만, "적립" 포인트가 남아있는 경우 조회
//            return queryFactory
//                    .selectFrom(QStore.store)
//                    .where(QStore.store.address.like(address + "%"));
//        });
//    }


    // HibernatePagingItemReader
//    @Bean(name = JOB_NAME + "_reader")
//    @StepScope
//    public HibernatePagingItemReader<Store> reader(@Value("#{jobParameters[address]}") final String address) {
//        final Map<String, Object> parameters = new LinkedHashMap<>();
//        parameters.put("address", address + "%");
//
//        return new HibernatePagingItemReaderBuilder<Store>()
//                .name(JOB_NAME + "_reader")
//                .queryString("SELECT s FROM Store s WHERE s.address LIKE :address")
//                .parameterValues(parameters)
//                .sessionFactory(entityManagerFactory.unwrap(SessionFactory.class))
//                .fetchSize(chunkSize)
//                .useStatelessSession(false)
//                .build();
//    }

    @Bean(name = JOB_NAME + "_processor")
    @StepScope
    public ItemProcessor<Store, StoreHistory> processor() {
        return item -> new StoreHistory(item, item.getProducts(), item.getEmployees());
    }

    public JpaItemWriter<StoreHistory> writer() {
        return new JpaItemWriterBuilder<StoreHistory>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }
}