package seol.study.springbatch.job;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import seol.study.springbatch.domain.*;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by jojoldu@gmail.com on 2017. 10. 27.
 * Blog : http://jojoldu.tistory.com
 * Github : https://github.com/jojoldu
 */

@SpringBootTest
@TestPropertySource(properties = "job.name=storeBackupBatch")
@TestPropertySource(properties = "chunkSize=2")
class StoreBackupBatchConfigurationTest {

    @Autowired
    JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    StoreRepository storeRepository;

    @Autowired
    StoreHistoryRepository storeHistoryRepository;

    @Test
    @DisplayName("Store 정보가 StoreHistory로 복사된다")
    void storeBackupBatch() throws Exception {
        // given:
        storeRepository.deleteAll();

        final Store store1 = new Store("서점", "서울시 강남구");
        store1.addProduct(new Product("책1_1", 10000L));
        store1.addProduct(new Product("책1_2", 20000L));
        store1.addEmployee(new Employee("직원1", LocalDate.now()));
        storeRepository.save(store1);

        final Store store2 = new Store("서점2", "서울시 강남구");
        store2.addProduct(new Product("책2_1", 10000L));
        store2.addProduct(new Product("책2_2", 20000L));
        store2.addEmployee(new Employee("직원2", LocalDate.now()));
        storeRepository.save(store2);

//        final Store store3 = new Store("서점3", "서울시 강남구");
//        store3.addProduct(new Product("책3_1", 10000L));
//        store3.addProduct(new Product("책3_2", 20000L));
//        store3.addEmployee(new Employee("직원3", LocalDate.now()));
//        storeRepository.save(store3);

        final JobParameters jobParameters = new JobParametersBuilder()
                .addString("address", "서울")
                .toJobParameters();
        // when:
        final JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then:
        assertThat(jobExecution.getStatus() == BatchStatus.COMPLETED).isTrue();

    }

    @Test
    @DisplayName("Chunk 단위로 롤백된다")
    void chunkRollback() throws Exception {
        // given:
        storeRepository.deleteAll();

        final Store store1 = new Store("서점", "서울시 강남구");
        store1.addProduct(new Product("책1_1", 10000L));
        store1.addProduct(new Product("책1_2", 20000L));
        store1.addEmployee(new Employee("직원1", LocalDate.now()));
        storeRepository.save(store1);

        final Store store2 = new Store("서점2", "서울시 강남구");
        store2.addProduct(new Product("책2_1", 10000L));
        store2.addProduct(new Product("책2_2", 20000L));
        store2.addEmployee(new Employee("직원2", LocalDate.now()));
        storeRepository.save(store2);

        final Store store3 = new Store("서점3", "서울시 강남구");
        store3.addProduct(new Product("책3_1", 10000L));
        store3.addProduct(new Product("책3_2", 20000L));
        store3.addEmployee(new Employee("직원3", LocalDate.now()));
        storeRepository.save(store3);

        final JobParameters jobParameters = new JobParametersBuilder()
                .addString("address", "서울")
                .toJobParameters();
        // when:
        final JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then:
        assertThat(jobExecution.getStatus() == BatchStatus.FAILED).isTrue();

        // 3개를 넣을때 Exception이 발생하고 2개만 저장
        assertThat(storeHistoryRepository.findAll().size()).isEqualTo(2);
    }
}