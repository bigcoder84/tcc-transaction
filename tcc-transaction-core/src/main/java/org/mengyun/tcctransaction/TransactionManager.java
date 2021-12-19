package org.mengyun.tcctransaction;

import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.TransactionType;
import org.mengyun.tcctransaction.repository.TransactionRepository;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 事务管理器，提供事务的获取、发起、提交、回滚，参与者的新增等等方法。
 * Created by changmingxie on 10/26/15.
 */
public class TransactionManager {

    static final org.slf4j.Logger logger = LoggerFactory.getLogger(TransactionManager.class.getSimpleName());
    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>();


    private int threadPoolSize = Runtime.getRuntime().availableProcessors() * 2 + 1;

    private int threadQueueSize = 1024;

    private ExecutorService asyncTerminatorExecutorService = new ThreadPoolExecutor(threadPoolSize,
            threadPoolSize,
            0l,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(threadQueueSize), new ThreadPoolExecutor.AbortPolicy());

    private ExecutorService asyncSaveExecutorService = new ThreadPoolExecutor(threadPoolSize,
            threadPoolSize,
            0l,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(threadQueueSize * 2), new ThreadPoolExecutor.CallerRunsPolicy());

    private TransactionRepository transactionRepository;


    public TransactionManager() {
    }

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    public Transaction begin(Object uniqueIdentify) {
        Transaction transaction = new Transaction(uniqueIdentify, TransactionType.ROOT);

        //for performance tuning, at create stage do not persistent
//        transactionRepository.create(transaction);
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 发起根事务
     * @return
     */
    public Transaction begin() {
        Transaction transaction = new Transaction(TransactionType.ROOT);
        //for performance tuning, at create stage do not persistent
//        transactionRepository.create(transaction);
        // 注册事务
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 发起分支事务。该方法在调用方法类型为 ParticipantRole.PROVIDER 并且 事务处于 Try 阶段被调用
     * @param transactionContext
     * @return
     */
    public Transaction propagationNewBegin(TransactionContext transactionContext) {
        // 创建 分支事务
        Transaction transaction = new Transaction(transactionContext);

        //for performance tuning, at create stage do not persistent
//        transactionRepository.create(transaction);
        //注册 事务
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 获取分支事务。该方法在调用方法类型为 ParticipantRole.PROVIDER 并且 事务处于 Confirm / Cancel 阶段被调用
     * @param transactionContext
     * @return
     * @throws NoExistedTransactionException
     */
    public Transaction propagationExistBegin(TransactionContext transactionContext) throws NoExistedTransactionException {
        // 查询事务
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid());

        if (transaction != null) {
            registerTransaction(transaction);
            return transaction;
        } else {
            throw new NoExistedTransactionException();
        }
    }

    /**
     * 添加参与者到事务
     * @param participant
     */
    public void enlistParticipant(Participant participant) {
        Transaction transaction = this.getCurrentTransaction();
        transaction.enlistParticipant(participant);

        if (transaction.getVersion() == 0l) {
            // transaction.getVersion() is zero which means never persistent before, need call create to persistent.
            transactionRepository.create(transaction);
        } else {
            transactionRepository.update(transaction);
        }
    }

    public void commit(boolean asyncCommit) {
        // 获取事务
        final Transaction transaction = getCurrentTransaction();
        // 设置事务状态为confirm
        transaction.changeStatus(TransactionStatus.CONFIRMING);
        // 更新事务
        transactionRepository.update(transaction);

        if (asyncCommit) {
            try {
                Long statTime = System.currentTimeMillis();

                asyncTerminatorExecutorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        commitTransaction(transaction);
                    }
                });
                logger.debug("async submit cost time:" + (System.currentTimeMillis() - statTime));
            } catch (Throwable commitException) {
                logger.warn("compensable transaction async submit confirm failed, recovery job will try to confirm later.", commitException.getCause());
                //throw new ConfirmingException(commitException);
            }
        } else {
            // 提交事务
            commitTransaction(transaction);
        }
    }


    public void rollback(boolean asyncRollback) {
        // 获取事务
        final Transaction transaction = getCurrentTransaction();
        // 设置事务状态为 CANCELLING
        transaction.changeStatus(TransactionStatus.CANCELLING);
        // 更新事务记录
        transactionRepository.update(transaction);

        if (asyncRollback) {
            // 是否是异步操作
            try {
                asyncTerminatorExecutorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        rollbackTransaction(transaction);
                    }
                });
            } catch (Throwable rollbackException) {
                logger.warn("compensable transaction async rollback failed, recovery job will try to rollback later.", rollbackException);
                throw new CancellingException(rollbackException);
            }
        } else {
            // 回滚事务
            rollbackTransaction(transaction);
        }
    }


    /**
     * 提交事务
     * @param transaction
     */
    private void commitTransaction(Transaction transaction) {
        try {
            // 提交事务
            transaction.commit();
            // 删除事务记录
            transactionRepository.delete(transaction);
        } catch (Throwable commitException) {

            //try save updated transaction
            try {
                transactionRepository.update(transaction);
            } catch (Exception e) {
                //ignore any exception here
            }

            logger.warn("compensable transaction confirm failed, recovery job will try to confirm later.", commitException);
            throw new ConfirmingException(commitException);
        }
    }

    private void rollbackTransaction(Transaction transaction) {
        try {
            // 事务回滚
            transaction.rollback();
            // 删除事务记录
            transactionRepository.delete(transaction);
        } catch (Throwable rollbackException) {

            //try save updated transaction
            try {
                transactionRepository.update(transaction);
            } catch (Exception e) {
                //ignore any exception here
            }
            
            logger.warn("compensable transaction rollback failed, recovery job will try to rollback later.", rollbackException);
            throw new CancellingException(rollbackException);
        }
    }

    /**
     * 获取当前线程 事务队列的队头事务
     * tips: registerTransaction是将事务注册到队列头部
     * @return
     */
    public Transaction getCurrentTransaction() {
        if (isTransactionActive()) {
            return CURRENT.get().peek();
        }
        return null;
    }

    /**
     * 判断当前线程是否在事务中
     * @return
     */
    public boolean isTransactionActive() {
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }


    /**
     * 注册事务到当前线程事务队列
     * @param transaction
     */
    private void registerTransaction(Transaction transaction) {

        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<Transaction>());
        }

        CURRENT.get().push(transaction);
    }

    public void cleanAfterCompletion(Transaction transaction) {
        if (isTransactionActive() && transaction != null) {
            Transaction currentTransaction = getCurrentTransaction();
            if (currentTransaction == transaction) {
                CURRENT.get().pop();
                if (CURRENT.get().size() == 0) {
                    CURRENT.remove();
                }
            } else {
                throw new SystemException("Illegal transaction when clean after completion");
            }
        }
    }


    public void changeStatus(TransactionStatus status) {
        changeStatus(status, false);
    }

    public void changeStatus(TransactionStatus status, boolean asyncSave) {
        Transaction transaction = this.getCurrentTransaction();
        transaction.setStatus(status);

        if (asyncSave) {
            asyncSaveExecutorService.submit(new AsyncSaveTask(transaction));
        } else {
            transactionRepository.update(transaction);
        }
    }

    class AsyncSaveTask implements Runnable {

        private Transaction transaction;

        public AsyncSaveTask(Transaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public void run() {

            //only can be TRY_SUCCESS
            try {
                if (transaction != null && transaction.getStatus().equals(TransactionStatus.TRY_SUCCESS)) {

                    Transaction foundTransaction = transactionRepository.findByXid(transaction.getXid());

                    if (foundTransaction != null && foundTransaction.getStatus().equals(TransactionStatus.TRYING)) {
                        transactionRepository.update(transaction);
                    }
                }
            } catch (Exception e) {
                //ignore the exception
            }
        }
    }

}
