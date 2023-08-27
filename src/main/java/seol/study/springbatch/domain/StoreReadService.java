package seol.study.springbatch.domain;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import java.util.List;


@Slf4j
@RequiredArgsConstructor
@Service
public class StoreReadService {

    private final StoreRepository storeRepository;
    private final EntityManagerFactory entityManagerFactory;
//    private final EntityManager entityManager;

    //    @Transactional(readOnly = true)
//    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<Store> find() {
        final var entityManager = entityManagerFactory.createEntityManager();
        final EntityTransaction tx = entityManager.getTransaction();
        tx.begin();

        entityManager.flush();
        entityManager.clear();


        final List<Store> stores = storeRepository.findAllByFetchJoin();
//        final long productSum = stores.stream()
//                .map(Store::getProducts)
//                .flatMap(Collection::stream)
//                .mapToLong(Product::getPrice)
//                .sum();
//
//        final var names = stores.stream()
//                .map(Store::getEmployees)
//                .flatMap(Collection::stream)
//                .map(Employee::getName)
//                .collect(Collectors.toList());
//        log.info("productSum: {}", productSum);
//        log.info("names: {}", names);

        tx.commit();
        entityManager.close();
        return stores;
    }
}