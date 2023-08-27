package seol.study.springbatch.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.HibernateCursorItemReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.HibernateCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import seol.study.springbatch.domain.Store;
import seol.study.springbatch.domain.StoreHistory;

import javax.persistence.EntityManagerFactory;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by jojoldu@gmail.com on 2017. 10. 27.
 * Blog : http://jojoldu.tistory.com
 * Github : https://github.com/jojoldu
 */
@Slf4j
@RequiredArgsConstructor
@Configuration
@ConditionalOnProperty(name = "job.name", havingValue = StoreBackupBatchConfig.JOB_NAME)
public class StoreBackupBatchConfig {

    public static final String JOB_NAME = "storeBackupBatch";
    private static int count = 0;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;

    private static String ADDRESS_PARAM = null;

    private int chunkSize;

    @Value("${chunkSize:10}")
    public void setChunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
    }


    @Bean(name = JOB_NAME)
    public Job job() {
        return jobBuilderFactory.get(JOB_NAME)
                .start(step())
                .build();
    }

    @Bean(name = JOB_NAME + "_step")
    @JobScope
    public Step step() {
        return stepBuilderFactory.get(JOB_NAME + "_step")
                .<Store, StoreHistory>chunk(chunkSize)
                .reader(reader(ADDRESS_PARAM))
                .processor(processor())
                .writer(writer())
                .build();
    }

//    @Bean(name = JOB_NAME+"_reader")
//    @StepScope
//    public JpaPagingItemReader<Store> reader (
//            @Value("#{jobParameters[address]}") String address) {
//
//        Map<String, Object> parameters = new LinkedHashMap<>();
//        parameters.put("address", address+"%");
//
//        return new JpaPagingItemReaderBuilder<Store>()
//                .pageSize(chunkSize)
//                .parameterValues(parameters)
//                .queryString("SELECT s FROM Store s WHERE s.address LIKE :address order by s.id")
//                .entityManagerFactory(entityManagerFactory)
//                .name(JOB_NAME+"_reader")
//                .transacted(false)
//                .build();
//    }

//    @Bean
//    @StepScope
//    public JpaPagingFetchItemReader<Store> reader(
//            @Value("#{jobParameters[address]}") String address) {
//
//        Map<String, Object> parameters = new LinkedHashMap<>();
//        parameters.put("address", address + "%");
//
//        JpaPagingFetchItemReader<Store> reader = new JpaPagingFetchItemReader<>();
//        reader.setEntityManagerFactory(entityManagerFactory);
//        reader.setQueryString("SELECT s FROM Store s WHERE s.address LIKE :address order by s.id");
//        reader.setParameterValues(parameters);
//        reader.setPageSize(chunkSize);
//
//        return reader;
//    }

//    @Bean
//    @StepScope
//    public QuerydslPagingItemReader<Store> reader(@Value("#{jobParameters[address]}") String address){
//        return new QuerydslPagingItemReader<>(entityManagerFactory, chunkSize, queryFactory -> {
//            // 요청 시간 기준으로 만료 기간이 지났지만, "적립" 포인트가 남아있는 경우 조회
//            return queryFactory
//                    .selectFrom(store)
//                    .where(store.address.like(address+"%"));
//        });
//    }

//    @Bean
//    @StepScope
//    public QuerydslCursorItemReader<Store> reader(@Value("#{jobParameters[address]}") String address){
//        return new QuerydslCursorItemReader<>(entityManagerFactory, chunkSize, queryFactory -> {
//            // 요청 시간 기준으로 만료 기간이 지났지만, "적립" 포인트가 남아있는 경우 조회
//            return queryFactory
//                    .selectFrom(store)
//                    .where(store.address.like(address+"%"));
//        });
//    }

    @Bean(name = JOB_NAME + "_reader")
    @StepScope
    public HibernateCursorItemReader<Store> reader(@Value("#{jobParameters[address]}") final String address) {
        final Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("address", address + "%");

        return new HibernateCursorItemReaderBuilder<Store>()
                .queryString("SELECT s FROM Store s WHERE s.address LIKE :address")
                .parameterValues(parameters)
                .fetchSize(chunkSize)
                .sessionFactory(entityManagerFactory.unwrap(SessionFactory.class))
                .useStatelessSession(false)
                .name(JOB_NAME + "_reader")
                .build();
    }

//    @Bean(name = JOB_NAME+"_reader")
//    @StepScope
//    public HibernatePagingItemReader<Store> reader(@Value("#{jobParameters[address]}") String address) {
//        Map<String, Object> parameters = new LinkedHashMap<>();
//        parameters.put("address", address + "%");
//
//        return new HibernatePagingItemReaderBuilder<Store>()
//                .queryString("SELECT s FROM Store s WHERE s.address LIKE :address")
//                .parameterValues(parameters)
//                .sessionFactory(entityManagerFactory.unwrap(SessionFactory.class))
//                .fetchSize(chunkSize)
//                .build();
//    }

    @Bean(name = JOB_NAME + "_processor")
    @StepScope
    public ItemProcessor<Store, StoreHistory> processor() {
        return item -> {
            count++;
            log.info("count={}", count);
            if (count > 2) {
                throw new IllegalStateException("야호");
            }
            return new StoreHistory(item, item.getProducts(), item.getEmployees());
        };
    }

    public JpaItemWriter<StoreHistory> writer() {
        return new JpaItemWriterBuilder<StoreHistory>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }
}