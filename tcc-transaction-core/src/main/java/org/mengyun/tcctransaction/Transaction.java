package org.mengyun.tcctransaction;


import org.mengyun.tcctransaction.api.ParticipantStatus;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.api.TransactionXid;
import org.mengyun.tcctransaction.common.TransactionType;

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by changmingxie on 10/26/15.
 */
public class Transaction implements Serializable {

    private static final long serialVersionUID = 7291423944314337931L;
    /**
     * 事务创建时间
     */
    private final Date createTime = new Date();
    /**
     * 参与者集合
     */
    private final List<Participant> participants = new ArrayList<Participant>();
    /**
     * 附带属性映射
     */
    private final Map<String, Object> attachments = new ConcurrentHashMap<String, Object>();
    private TransactionXid xid;
    /**
     * 事务状态
     */
    private TransactionStatus status;
    /**
     * 事务类型
     */
    private TransactionType transactionType;
    /**
     * 重试次数
     */
    private volatile int retriedCount = 0;
    /**
     * 最后更新时间
     */
    private Date lastUpdateTime = new Date();
    /**
     * 版本号
     */
    private long version = 0;
    private TransactionXid rootXid;

    public Transaction() {

    }

    /**
     * 创建分支事务
     * @param transactionContext
     */
    public Transaction(TransactionContext transactionContext) {
        this.xid = transactionContext.getXid();
        this.rootXid = transactionContext.getRootXid();

        this.status = TransactionStatus.TRYING;
        this.transactionType = TransactionType.BRANCH;
    }

    /**
     * 创建指定类型的根事务
     * @param transactionType
     */
    public Transaction(TransactionType transactionType) {
        this(null, transactionType);
    }

    public Transaction(Object uniqueIdentity, TransactionType transactionType) {

        this.xid = new TransactionXid(uniqueIdentity);
        this.status = TransactionStatus.TRYING;
        this.transactionType = transactionType;

        if (transactionType.equals(TransactionType.ROOT)) {
            this.rootXid = xid;
        }
    }

    /**
     * 添加参与者
     * @param participant
     */
    public void enlistParticipant(Participant participant) {
        participants.add(participant);
    }


    public Xid getXid() {
        return xid.clone();
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public List<Participant> getParticipants() {
        return participants;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void changeStatus(TransactionStatus status) {
        this.status = status;
    }

    /**
     * 提交TCC事务
     */
    public void commit() {
        for (Participant participant : participants) {
            if (!participant.getStatus().equals(ParticipantStatus.CONFIRM_SUCCESS)) {
                participant.commit();
                participant.setStatus(ParticipantStatus.CONFIRM_SUCCESS);
            }
        }
    }

    /**
     * 回滚TCC事务
     */
    public void rollback() {
        for (Participant participant : participants) {
            if (!participant.getStatus().equals(ParticipantStatus.CANCEL_SUCCESS)) {
                participant.rollback();
                participant.setStatus(ParticipantStatus.CANCEL_SUCCESS);
            }
        }
    }

    public int getRetriedCount() {
        return retriedCount;
    }

    public void setRetriedCount(int retriedCount) {
        this.retriedCount = retriedCount;
    }

    public synchronized void addRetriedCount() {
        this.retriedCount++;
    }

    public Map<String, Object> getAttachments() {
        return attachments;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void updateVersion() {
        this.version++;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date date) {
        this.lastUpdateTime = date;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void updateTime() {
        this.lastUpdateTime = new Date();
    }

    public boolean isTryFailed() {
        for (Participant participant : participants) {
            if (participant.getStatus().equals(ParticipantStatus.TRY_FAILED)) {
                return true;
            }
        }
        return false;
    }

    public TransactionXid getRootXid() {
        return rootXid;
    }

    public void setRootXid(TransactionXid rootXid) {
        this.rootXid = rootXid;
    }
}
