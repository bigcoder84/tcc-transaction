package org.mengyun.tcctransaction.interceptor;

import org.mengyun.tcctransaction.InvocationContext;
import org.mengyun.tcctransaction.Participant;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionManager;
import org.mengyun.tcctransaction.api.*;
import org.mengyun.tcctransaction.common.ParticipantRole;
import org.mengyun.tcctransaction.support.FactoryBuilder;
import org.mengyun.tcctransaction.utils.ReflectionUtils;

/**
 * Created by changmingxie on 11/8/15.
 */
public class ResourceCoordinatorInterceptor {

    private TransactionManager transactionManager;

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public Object interceptTransactionContextMethod(TransactionMethodJoinPoint pjp) throws Throwable {

        Transaction transaction = transactionManager.getCurrentTransaction();
        if (transaction != null && transaction.getStatus().equals(TransactionStatus.TRYING)) {
            // 添加事务参与者
            Participant participant = enlistParticipant(pjp);
            if (participant != null) {
                Object result = null;
                try {
                    // 执行原方法逻辑
                    result = pjp.proceed(pjp.getArgs());
                    participant.setStatus(ParticipantStatus.TRY_SUCCESS);
                } catch (Throwable e) {
                    participant.setStatus(ParticipantStatus.TRY_FAILED);
                    //if root transaction, here no need persistent transaction
                    // because following stage is rollback, transaction's status is changed to CANCELING and save
//                    transactionManager.update(participant);
                    throw e;
                }
                return result;
            }
        }
        return pjp.proceed(pjp.getArgs());
    }

    private Participant enlistParticipant(TransactionMethodJoinPoint pjp) {

        Transaction transaction = transactionManager.getCurrentTransaction();
        CompensableMethodContext compensableMethodContext = new CompensableMethodContext(pjp, transaction);

        if (compensableMethodContext.getParticipantRole().equals(ParticipantRole.NORMAL)) {
            return null;
        }
        // 获得 确认执行业务方法 和 取消执行业务方法
        String confirmMethodName = compensableMethodContext.getConfirmMethodName();
        String cancelMethodName = compensableMethodContext.getCancelMethodName();
        // 获取事务上下文编辑器
        Class<? extends TransactionContextEditor> transactionContextEditorClass = compensableMethodContext.getTransactionContextEditorClass();
        // 创建事务ID
        TransactionXid xid = new TransactionXid(transaction.getXid().getGlobalTransactionId());

        if (compensableMethodContext.getTransactionContext() == null) {
            // todo 实例化 事务上下文编辑器，并设置
            FactoryBuilder.factoryOf(transactionContextEditorClass).getInstance().set(new TransactionContext(transaction.getRootXid(), xid, TransactionStatus.TRYING.getId(), ParticipantStatus.TRYING.getId()), pjp.getTarget(), pjp.getMethod(), pjp.getArgs());
        }
        // todo 获得声明 @Compensable 方法的实际类
        Class targetClass = ReflectionUtils.getDeclaringType(pjp.getTarget().getClass(), compensableMethodContext.getMethod().getName(), compensableMethodContext.getMethod().getParameterTypes());

        // 创建 确认执行方法调用上下文 和 取消执行方法调用上下文
        InvocationContext confirmInvocation = new InvocationContext(targetClass,
                confirmMethodName,
                compensableMethodContext.getMethod().getParameterTypes(), pjp.getArgs());

        InvocationContext cancelInvocation = new InvocationContext(targetClass,
                cancelMethodName,
                compensableMethodContext.getMethod().getParameterTypes(), pjp.getArgs());

        // 创建事务参与者
        Participant participant =
                new Participant(
                        transaction.getRootXid(),
                        xid,
                        confirmInvocation,
                        cancelInvocation,
                        transactionContextEditorClass);
        // 添加 事务参与者 到 事务
        transactionManager.enlistParticipant(participant);
        return participant;
    }
}
