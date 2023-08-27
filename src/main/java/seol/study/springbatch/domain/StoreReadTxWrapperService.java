package seol.study.springbatch.domain;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;

import javax.persistence.EntityManager;
import java.util.Collection;
import java.util.stream.Collectors;


@Slf4j
@RequiredArgsConstructor
@Service
public class StoreReadTxWrapperService {

    private final StoreReadService storeReadService;
    private final EntityManager entityManager;
    private final PlatformTransactionManager transactionManager;

    //    @Transactional
    public long find() {
        final var transactionStatus = transactionManager.getTransaction(new DefaultTransactionAttribute() {
            @Override
            public boolean rollbackOn(final Throwable ex) {
                return true;
            }
        });
        final var stores = storeReadService.find();
        final long productSum = stores.stream()
                .map(Store::getProducts)
                .flatMap(Collection::stream)
                .mapToLong(Product::getPrice)
                .sum();

        final var names = stores.stream()
                .map(Store::getEmployees)
                .flatMap(Collection::stream)
                .map(Employee::getName)
                .collect(Collectors.toList());
        log.info("productSum: {}", productSum);
        log.info("names: {}", names);


        transactionManager.commit(transactionStatus);
        return productSum;
    }


}