package seol.study.springbatch.job;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import seol.study.springbatch.domain.StoreRepository;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@TestPropertySource(properties = "job.name=n1JpaPagingJob2")
class N1JpaPaging2JobConfigTest {

    @Autowired
    JobLauncherTestUtils jobLauncherTestUtils;
    @Autowired
    StoreRepository storeRepository;

    @Test
    void n1JpaPagingJob2() throws Exception {
        // given:
        storeRepository.deleteAll();

        final JobParameters jobParameters = new JobParametersBuilder()
                .addString("address", "서울")
                .toJobParameters();
        // when:
        final JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then:
        assertThat(jobExecution.getStatus() == BatchStatus.COMPLETED).isTrue();

    }
}