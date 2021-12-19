package org.mengyun.tcctransaction.interceptor;

import com.alibaba.fastjson.JSON;
import org.mengyun.tcctransaction.IllegalTransactionStatusException;
import org.mengyun.tcctransaction.NoExistedTransactionException;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.api.ParticipantStatus;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * 可补偿事务拦截器
 * Created by changmingxie on 10/30/15.
 */
public class CompensableTransactionInterceptor {

    static final Logger logger = LoggerFactory.getLogger(CompensableTransactionInterceptor.class.getSimpleName());

    private TransactionManager transactionManager;

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public Object interceptCompensableMethod(TransactionMethodJoinPoint pjp) throws Throwable {

        Transaction transaction = transactionManager.getCurrentTransaction();
        CompensableMethodContext compensableMethodContext = new CompensableMethodContext(pjp, transaction);

        // if method is @Compensable and no transaction context and no transaction, then root
        // else if method is @Compensable and has transaction context and no transaction ,then provider
        switch (compensableMethodContext.getParticipantRole()) {
            case ROOT:
                // 根事务
                return rootMethodProceed(compensableMethodContext);
            case PROVIDER:
                // 分支事务
                return providerMethodProceed(compensableMethodContext);
            default:
                return compensableMethodContext.proceed();
        }
    }

    /**
     * 发起根事务
     * @param compensableMethodContext
     * @return
     * @throws Throwable
     */
    private Object rootMethodProceed(CompensableMethodContext compensableMethodContext) throws Throwable {
        Object returnValue = null;
        Transaction transaction = null;
        boolean asyncConfirm = compensableMethodContext.getAnnotation().asyncConfirm();
        boolean asyncCancel = compensableMethodContext.getAnnotation().asyncCancel();
        try {
            // 发起根事务
            transaction = transactionManager.begin(compensableMethodContext.getUniqueIdentity());
            try {
                // 执行方法原逻辑
                returnValue = compensableMethodContext.proceed();
            } catch (Throwable tryingException) {
                // 回滚事务
                transactionManager.rollback(asyncCancel);
                throw tryingException;
            }
            // 提交事务
            transactionManager.commit(asyncConfirm);
        } finally {
            // 将事务从当前线程事务队列移除
            transactionManager.cleanAfterCompletion(transaction);
        }
        return returnValue;
    }

    private Object providerMethodProceed(CompensableMethodContext compensableMethodContext) throws Throwable {

        Transaction transaction = null;
        boolean asyncConfirm = compensableMethodContext.getAnnotation().asyncConfirm();
        boolean asyncCancel = compensableMethodContext.getAnnotation().asyncCancel();

        try {
            switch (TransactionStatus.valueOf(compensableMethodContext.getTransactionContext().getStatus())) {
                case TRYING:
                    // 传播发起分支事务
                    transaction = transactionManager.propagationNewBegin(compensableMethodContext.getTransactionContext());
                    Object result = null;
                    try {
                        result = compensableMethodContext.proceed();
                        //TODO: need tuning here, async change the status to tuning the invoke chain performance
                        //transactionManager.changeStatus(TransactionStatus.TRY_SUCCESS, asyncSave);
                        // 将状态变为 TRY_SUCCESS
                        transactionManager.changeStatus(TransactionStatus.TRY_SUCCESS, true);
                    } catch (Throwable e) {
                        transactionManager.changeStatus(TransactionStatus.TRY_FAILED);
                        throw e;
                    }
                    return result;
                case CONFIRMING:
                    try {
                        // 传播获取分支事务
                        transaction = transactionManager.propagationExistBegin(compensableMethodContext.getTransactionContext());
                        // 提交事务
                        transactionManager.commit(asyncConfirm);
                    } catch (NoExistedTransactionException excepton) {
                        //the transaction has been commit,ignore it.
                        logger.info("no existed transaction found at CONFIRMING stage, will ignore and confirm automatically. transaction:" + JSON.toJSONString(transaction));
                    }
                    break;
                case CANCELLING:
                    try {
                        //The transaction' status of this branch transaction, passed from consumer side.
                        //该分支事务的事务状态，从消费者端传递过来。
                        int transactionStatusFromConsumer = compensableMethodContext.getTransactionContext().getParticipantStatus();
                        transaction = transactionManager.propagationExistBegin(compensableMethodContext.getTransactionContext());

                        // Only if transaction's status is at TRY_SUCCESS、TRY_FAILED、CANCELLING stage we can call rollback.
                        // If transactionStatusFromConsumer is TRY_SUCCESS, no mate current transaction is TRYING or not, also can rollback.
                        // transaction's status is TRYING while transactionStatusFromConsumer is TRY_SUCCESS may happen when transaction's changeStatus is async.
                        if (transaction.getStatus().equals(TransactionStatus.TRY_SUCCESS)
                                || transaction.getStatus().equals(TransactionStatus.TRY_FAILED)
                                || transaction.getStatus().equals(TransactionStatus.CANCELLING)
                                || transactionStatusFromConsumer == ParticipantStatus.TRY_SUCCESS.getId()) {
                            // 回滚事务
                            transactionManager.rollback(asyncCancel);
                        } else {
                            //in this case, transaction's Status is TRYING and transactionStatusFromConsumer is TRY_FAILED
                            // this may happen if timeout exception throws during rpc call.
                            throw new IllegalTransactionStatusException("Branch transaction status is TRYING, cannot rollback directly, waiting for recovery job to rollback.");
                        }

                    } catch (NoExistedTransactionException exception) {
                        //the transaction has been rollback,ignore it.
                        logger.info("no existed transaction found at CANCELLING stage, will ignore and cancel automatically. transaction:" + JSON.toJSONString(transaction));
                    }
                    break;
            }

        } finally {
            // 将事务从当前线程事务队列移除
            transactionManager.cleanAfterCompletion(transaction);
        }

        // 如果是CONFIRMING 或 CANCEL 则返回空值
        Method method = compensableMethodContext.getMethod();
        return ReflectionUtils.getNullValue(method.getReturnType());
    }
}
