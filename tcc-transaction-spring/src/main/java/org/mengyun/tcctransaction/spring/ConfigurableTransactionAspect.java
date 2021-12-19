package org.mengyun.tcctransaction.spring;

import org.aspectj.lang.annotation.Aspect;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.interceptor.CompensableTransactionAspect;
import org.mengyun.tcctransaction.interceptor.CompensableTransactionInterceptor;
import org.mengyun.tcctransaction.support.TransactionConfigurator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;

import javax.annotation.PostConstruct;

/**
 * 实现Ordered，用于指定切面顺序
 * Created by changmingxie on 10/30/15.
 */
@Aspect
public class ConfigurableTransactionAspect extends CompensableTransactionAspect implements Ordered {

    @Autowired
    private TransactionConfigurator transactionConfigurator;

    @PostConstruct
    public void init() {

        TransactionManager transactionManager = transactionConfigurator.getTransactionManager();

        CompensableTransactionInterceptor compensableTransactionInterceptor = new CompensableTransactionInterceptor();
        compensableTransactionInterceptor.setTransactionManager(transactionManager);

        this.setCompensableTransactionInterceptor(compensableTransactionInterceptor);
    }

    /**
     * 指定切面顺序，AOP可以看做一个同心圆，圆心就是我们被代理的方法，当切面类getOrder方法返回值越小，则切面越在外层
     * @return
     */
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }
}
