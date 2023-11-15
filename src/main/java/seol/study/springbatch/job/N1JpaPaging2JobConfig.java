package seol.study.springbatch.job;

import java.util.Objects;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import seol.study.springbatch.common.QuerydslPagingItemBatchSizeReader;
import seol.study.springbatch.common.QuerydslPagingItemWithN1Reader;
import seol.study.springbatch.domain.QStore;
import seol.study.springbatch.domain.Store;
import seol.study.springbatch.domain.StoreHistory;
import seol.study.springbatch.domain.StoreWriteService;


@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = N1JpaPaging2JobConfig.JOB_NAME)
public class N1JpaPaging2JobConfig {

    public static final String JOB_NAME = "n1JpaPagingJob2";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final StoreWriteService storeWriteService;
    private final EntityManager entityManager;

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
                .reader(getAppliedBatchSizeReader(null))
//                .reader(getN1Reader(null))
                .processor(processor())
                .writer(writer())
                .allowStartIfComplete(true)
                .build();
    }

    // JpaPagingItemReader
//    @Bean(name = JOB_NAME + "_reader")
//    @StepScope
//    public JpaPagingItemReader<Store> reader(
//            @Value("#{jobParameters[address]}") final String address) {
//
//        final Map<String, Object> parameters = new LinkedHashMap<>();
////        parameters.put("address", address + "%");
//        parameters.put("address", "서울" + "%");
//
//        return new JpaPagingItemReaderBuilder<Store>()
//                .pageSize(chunkSize)
//                .parameterValues(parameters)
//                .queryString("SELECT s FROM Store s WHERE s.address LIKE :address order by s.id")
//                .entityManagerFactory(entityManagerFactory)
//                .name(JOB_NAME + "_reader")
//                .transacted(true)
//                .build();
//    }

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

    // QuerydslPagingItemV1Reader
    @Bean(name = JOB_NAME + "BatchSizeReader")
    @StepScope
    public QuerydslPagingItemBatchSizeReader<Store> getAppliedBatchSizeReader(@Value("#{jobParameters[address]}") final String address) {
        return new QuerydslPagingItemBatchSizeReader<>(entityManagerFactory, chunkSize, queryFactory -> {
            // 요청 시간 기준으로 만료 기간이 지났지만, "적립" 포인트가 남아있는 경우 조회
            return queryFactory
                    .selectFrom(QStore.store)
                    .where(QStore.store.address.like(address + "%"));
        });
    }

    // QuerydslPagingItemV2Reader
    @Bean(name = JOB_NAME + "N1Reader")
    @StepScope
    public QuerydslPagingItemWithN1Reader<Store> getN1Reader(@Value("#{jobParameters[address]}") final String address) {
        final var querydslPagingItemV2Reader = new QuerydslPagingItemWithN1Reader<Store>(
            entityManagerFactory, chunkSize, queryFactory -> {
            // 요청 시간 기준으로 만료 기간이 지났지만, "적립" 포인트가 남아있는 경우 조회
            return queryFactory
                .selectFrom(QStore.store)
                .where(QStore.store.address.like(address + "%"));
        });
        return querydslPagingItemV2Reader;
    }

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
        return item -> {
            log.info("entityManager.isOpen()={}", entityManager.isOpen());
            log.info("entityManager.contains(item)={}", entityManager.contains(item));

            if(Objects.nonNull(QuerydslPagingItemWithN1Reader.entityManager)) {
//            QuerydslPagingItemV2Reader.entityManager.close();
//            log.info("QuerydslPagingItemV2Reader.entityManager.isOpen()={}", QuerydslPagingItemV2Reader.entityManager.isOpen());
//            log.info("QuerydslPagingItemV2Reader.entityManager.contains={}", QuerydslPagingItemV2Reader.entityManager.contains(item));
            }
            final var storeHistory = new StoreHistory(item, item.getProducts(),
                item.getEmployees());
            log.info("storeHistory={}", storeHistory);
            return storeHistory;
        };
    }

    public JpaItemWriter<StoreHistory> writer() {
        return new JpaItemWriterBuilder<StoreHistory>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

}
