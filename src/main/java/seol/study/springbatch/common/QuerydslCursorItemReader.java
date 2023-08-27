package seol.study.springbatch.common;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.FactoryExpression;
import com.querydsl.jpa.FactoryExpressionTransformer;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import javax.persistence.EntityManagerFactory;
import java.lang.reflect.Method;
import java.util.function.Function;


public class QuerydslCursorItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
        implements InitializingBean {

    private ScrollableResults cursor;
    private boolean initialized = false;
    private int fetchSize;
    private SessionFactory sessionFactory;
    private Session statefulSession;
    private Function<JPAQueryFactory, JPAQuery<T>> queryFunction;

    public QuerydslCursorItemReader(final EntityManagerFactory emf, final int fetchSize, final Function<JPAQueryFactory, JPAQuery<T>> queryFunction) {
        setName(ClassUtils.getShortName(QuerydslCursorItemReader.class));
        this.sessionFactory = emf.unwrap(SessionFactory.class);
        this.fetchSize = fetchSize;
        this.queryFunction = queryFunction;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.state(fetchSize >= 0, "fetchSize must not be negative");
    }

    @Override
    protected T doRead() throws Exception {
        if (cursor.next()) {
            final Object[] data = cursor.get();

            if (data.length > 1) {
                // If there are multiple items this must be a projection
                // and T is an array type.
                @SuppressWarnings("unchecked") final T item = (T) data;
                return item;
            } else {
                // Assume if there is only one item that it is the data the user
                // wants.
                // If there is only one item this is going to be a nasty shock
                // if T is an array type but there's not much else we can do...
                @SuppressWarnings("unchecked") final T item = (T) data[0];
                return item;
            }

        }
        return null;
    }

    /**
     * Open hibernate session and create a forward-only cursor for the query.
     */
    @Override
    protected void doOpen() throws Exception {
        Assert.state(!initialized, "Cannot open an already opened ItemReader, call close first");
        cursor = getForwardOnlyCursor(fetchSize);
        initialized = true;
    }

    public ScrollableResults getForwardOnlyCursor(final int fetchSize) {
        final Query query = createQuery();
        return query.setFetchSize(fetchSize).scroll(ScrollMode.FORWARD_ONLY);
    }

    public Query createQuery() {

        if (statefulSession == null) {
            statefulSession = sessionFactory.openSession();
        }

        final JPAQueryFactory queryFactory = new JPAQueryFactory(statefulSession);
        final JPAQuery<T> jpaQuery = queryFunction.apply(queryFactory);
        final Query query = jpaQuery.createQuery().unwrap(Query.class);

//      set transformer, if necessary
        final Expression<?> projection = jpaQuery.getMetadata().getProjection();
        if (projection instanceof FactoryExpression) {
            query.setResultTransformer(new FactoryExpressionTransformer((FactoryExpression<?>) projection));
        }

        return query;
    }


    /**
     * Update the context and clear the session if stateful.
     *
     * @param executionContext the current {@link ExecutionContext}
     * @throws ItemStreamException if there is a problem
     */
    @Override
    public void update(final ExecutionContext executionContext) throws ItemStreamException {
        super.update(executionContext);
        clear();
    }

    public void clear() {
        if (statefulSession != null) {
            statefulSession.clear();
        }
    }


    /**
     * Wind forward through the result set to the item requested. Also clears
     * the session every now and then (if stateful) to avoid memory problems.
     * The frequency of session clearing is the larger of the fetch size (if
     * set) and 100.
     *
     * @param itemIndex the first item to read
     * @throws Exception if there is a problem
     * @see AbstractItemCountingItemStreamItemReader#jumpToItem(int)
     */
    @Override
    protected void jumpToItem(final int itemIndex) throws Exception {
        final int flushSize = Math.max(fetchSize, 100);
        jumpToItem(cursor, itemIndex, flushSize);
    }

    public void jumpToItem(final ScrollableResults cursor, final int itemIndex, final int flushInterval) {
        for (int i = 0; i < itemIndex; i++) {
            cursor.next();
            if (i % flushInterval == 0) {
                statefulSession.clear(); // Clears in-memory cache
            }
        }
    }


    /**
     * Close the cursor and hibernate session.
     */
    @Override
    protected void doClose() throws Exception {

        initialized = false;

        if (cursor != null) {
            cursor.close();
        }

        close();
    }

    public void close() {
        if (statefulSession != null) {
            final Method close = ReflectionUtils.findMethod(Session.class, "close");
            ReflectionUtils.invokeMethod(close, statefulSession);
            statefulSession = null;
        }
    }

}
