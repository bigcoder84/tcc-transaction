package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.*;

import java.io.Serializable;

/**
 * 事务参与者
 * Created by changmingxie on 10/27/15.
 */
public class Participant implements Serializable {

    private static final long serialVersionUID = 4127729421281425247L;
    Class<? extends TransactionContextEditor> transactionContextEditorClass;

    private TransactionXid rootXid;
    /**
     * 事务编号
     * <p>
     * 参与者事务编号。通过 TransactionXid.globalTransactionId 属性，
     * 关联上其所属的事务。当参与者进行远程调用时，远程的分支事务的事务编号等于该参与者的事务编号。
     * 通过事务编号的关联，TCC Confirm / Cancel 阶段，使用参与者的事务编号和远程的分支事务进行关联，
     * 从而实现事务的提交和回滚
     * <p/>
     */
    private TransactionXid xid;
    /**
     * 确认执行业务方法调用上下文
     */
    private InvocationContext confirmInvocationContext;
    /**
     * 取消执行业务方法
     */
    private InvocationContext cancelInvocationContext;
    private int status = ParticipantStatus.TRYING.getId();

    public Participant() {

    }

    public Participant(TransactionXid rootXid, TransactionXid xid, InvocationContext confirmInvocationContext, InvocationContext cancelInvocationContext, Class<? extends TransactionContextEditor> transactionContextEditorClass) {
        this.xid = xid;
        this.rootXid = rootXid;
        this.confirmInvocationContext = confirmInvocationContext;
        this.cancelInvocationContext = cancelInvocationContext;
        this.transactionContextEditorClass = transactionContextEditorClass;
    }

    public void rollback() {
        Terminator.invoke(new TransactionContext(rootXid, xid, TransactionStatus.CANCELLING.getId(), status), cancelInvocationContext, transactionContextEditorClass);
    }

    public void commit() {
        Terminator.invoke(new TransactionContext(rootXid, xid, TransactionStatus.CONFIRMING.getId(), status), confirmInvocationContext, transactionContextEditorClass);
    }

    public InvocationContext getConfirmInvocationContext() {
        return confirmInvocationContext;
    }

    public InvocationContext getCancelInvocationContext() {
        return cancelInvocationContext;
    }

    public void setStatus(ParticipantStatus status) {
        this.status = status.getId();
    }

    public ParticipantStatus getStatus() {
        return ParticipantStatus.valueOf(this.status);
    }
}
