package seol.study.springbatch.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

import static javax.persistence.CascadeType.ALL;


@NoArgsConstructor
@Getter
@Entity
public class Store {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;
    private String address;

//    @OneToMany(mappedBy = "store", cascade = ALL)
    @OneToMany(mappedBy = "store", cascade = ALL, fetch = FetchType.LAZY)
    private List<Product> products = new ArrayList<>();

//    @OneToMany(mappedBy = "store", cascade = ALL)
    @OneToMany(mappedBy = "store", cascade = ALL, fetch = FetchType.LAZY)
    private List<Employee> employees = new ArrayList<>();

    public Store(final String name, final String address) {
        this.name = name;
        this.address = address;
    }

    public void addProduct(final Product product) {
        this.products.add(product);
        product.updateStore(this);
    }

    public void addEmployee(final Employee employee) {
        this.employees.add(employee);
        employee.updateStore(this);
    }
}
