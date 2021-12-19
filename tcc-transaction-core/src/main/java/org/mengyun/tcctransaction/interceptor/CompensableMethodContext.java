package org.mengyun.tcctransaction.interceptor;

import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.api.Compensable;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionContextEditor;
import org.mengyun.tcctransaction.api.UniqueIdentity;
import org.mengyun.tcctransaction.common.ParticipantRole;
import org.mengyun.tcctransaction.support.FactoryBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Created by changming.xie on 04/04/19.
 */
public class CompensableMethodContext {

    TransactionMethodJoinPoint pjp = null;

    private Transaction transaction = null;

    TransactionContext transactionContext = null;

    Compensable compensable = null;

    public CompensableMethodContext(TransactionMethodJoinPoint pjp, Transaction transaction) {
        this.pjp = pjp;

        this.transaction = transaction;

        this.compensable = pjp.getCompensable();

        this.transactionContext = FactoryBuilder.factoryOf(pjp.getTransactionContextEditorClass()).getInstance().get(pjp.getTarget(), pjp.getMethod(), pjp.getArgs());
    }

    public Compensable getAnnotation() {
        return compensable;
    }

    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public Method getMethod() {
        return pjp.getMethod();
    }

    public Object getUniqueIdentity() {
        Annotation[][] annotations = this.getMethod().getParameterAnnotations();

        for (int i = 0; i < annotations.length; i++) {
            for (Annotation annotation : annotations[i]) {
                if (annotation.annotationType().equals(UniqueIdentity.class)) {

                    Object[] params = pjp.getArgs();
                    Object unqiueIdentity = params[i];

                    return unqiueIdentity;
                }
            }
        }

        return null;
    }

    /**
     *
     * 如果方法被@Compensable 注释，则表示需要tcc 事务，如果没有活动事务，则需要require new。
     * 如果方法不是@Compensable 注释，而是带有 TransactionContext 参数。
     *      如果有活动交易，这意味着需要参与者 tcc 交易。如果 transactionContext 为 null，则它将事务登记为 CONSUMER 角色，
     *      else 表示有另一个方法作为 Consumer 已经登记了事务，这个方法不需要登记。
     * @return
     */
    public ParticipantRole getParticipantRole() {

        //1. If method is @Compensable annotated, which means need tcc transaction, if no active transaction, need require new.
        //2. If method is not @Compensable annotated, but with TransactionContext Param.
        //   It means need participant tcc transaction if has active transaction. If transactionContext is null, then it enlist the transaction as CONSUMER role,
        //   else means there is another method roled as Consumer has enlisted the transaction, this method no need enlist.


        //Method is @Compensable annotated. Currently has no active transaction && no active transaction context,
        // then the method need enlist the transaction as ROOT role.
        //方法是@Compensable 注释的。当前没有活动事务 且 没有活动事务上下文，那么该方法需要将事务登记为 ROOT 角色
        if (compensable != null && transaction == null && transactionContext == null) {
            return ParticipantRole.ROOT;
        }


        //Method is @Compensable annotated. Currently has no active transaction, but has active transaction context.
        // This means there is a active transaction, need renew the transaction and enlist the transaction as PROVIDER role.
        // 方法是@Compensable 注释的。当前没有活动事务，但有活动事务上下文。这意味着有一个活跃的交易，需要更新交易并将交易登记为 PROVIDER 角色。
        if (compensable != null && transaction == null && transactionContext != null) {
            return ParticipantRole.PROVIDER;
        }

        //Method is @Compensable annotated, and has active transaction, but no transaction context.
        //then the method need enlist the transaction as CONSUMER role,
        // its role may be ROOT before if this method is the entrance of the tcc transaction.
        //方法是@Compensable 注释的，并且有活动事务，但没有事务上下文。那么该方法需要将交易登记为消费者角色，如果这个方法是tcc交易的入口，它的作用可能是ROOT之前。
        if (compensable != null && transaction != null && transactionContext == null) {
            return ParticipantRole.CONSUMER;
        }

        //Method is @Compensable annotated, and has active transaction, and also has transaction context.
        //then the method need enlist the transaction as CONSUMER role, its role maybe PROVIDER before.
        // 方法是@Compensable 注解，有活动事务，也有事务上下文。那么该方法需要将事务登记为 CONSUMER 角色，它的角色可能是之前的 PROVIDER。
        if (compensable != null && transaction != null && transactionContext != null) {
            return ParticipantRole.CONSUMER;
        }

        //Method is not @Compensable annotated, but with TransactionContext Param.
        // If currently there is a active transaction and transaction context is null,
        // then need enlist the transaction with CONSUMER role.
        // 方法没有@Compensable 注释，而是带有TransactionContext 参数。如果当前有一个活动事务并且事务上下文为空，然后需要使用 CONSUMER 角色登记事务。
        if (compensable == null && transaction != null && transactionContext == null) {
            return ParticipantRole.CONSUMER;
        }

        return ParticipantRole.NORMAL;
    }

    /**
     * 执行被代理方法
     * @return
     * @throws Throwable
     */
    public Object proceed() throws Throwable {
        return this.pjp.proceed();
    }

    public Class<? extends TransactionContextEditor> getTransactionContextEditorClass() {
        return pjp.getTransactionContextEditorClass();
    }

    public String getConfirmMethodName() {
        return compensable == null ? pjp.getMethod().getName() : compensable.confirmMethod();
    }

    public String getCancelMethodName() {
        return compensable == null ? pjp.getMethod().getName() : compensable.cancelMethod();
    }
}