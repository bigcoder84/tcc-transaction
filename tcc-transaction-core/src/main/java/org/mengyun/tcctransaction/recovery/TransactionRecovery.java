package org.mengyun.tcctransaction.recovery;


import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionOptimisticLockException;
import org.mengyun.tcctransaction.common.TransactionType;
import org.mengyun.tcctransaction.repository.LocalStorable;
import org.mengyun.tcctransaction.repository.Page;
import org.mengyun.tcctransaction.repository.SentinelTransactionRepository;
import org.mengyun.tcctransaction.repository.TransactionRepository;
import org.mengyun.tcctransaction.support.TransactionConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.mengyun.tcctransaction.api.TransactionStatus.CANCELLING;
import static org.mengyun.tcctransaction.api.TransactionStatus.CONFIRMING;


/**
 * 异常事务恢复逻辑
 * Created by changmingxie on 11/10/15.
 */
public class TransactionRecovery {

    public static final int CONCURRENT_RECOVERY_TIMEOUT = 60;

    public static final int MAX_ERROR_COUNT_SHREDHOLD = 15;

    static final Logger logger = LoggerFactory.getLogger(TransactionRecovery.class.getSimpleName());

    static volatile ExecutorService recoveryExecutorService = null;

    private TransactionConfigurator transactionConfigurator;

    /**
     * 触发最大重试打印计数
     */
    private AtomicInteger triggerMaxRetryPrintCount = new AtomicInteger();

    private AtomicInteger recoveryFailedPrintCount = new AtomicInteger();

    private volatile int logMaxPrintCount = MAX_ERROR_COUNT_SHREDHOLD;

    private Lock logSync = new ReentrantLock();

    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }

    /**
     * 恢复异常事务
     */
    public void startRecover() {
        // 初始化
        ensureRecoveryInitialized();
        // 获取事务存储器
        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();

        if (transactionRepository instanceof SentinelTransactionRepository) {
            SentinelTransactionRepository sentinelTransactionRepository = (SentinelTransactionRepository) transactionRepository;
            if (!sentinelTransactionRepository.getSentinelController().degrade()) {
                startRecover(sentinelTransactionRepository.getWorkTransactionRepository());
            }
            // 开始恢复
            startRecover(sentinelTransactionRepository.getDegradedTransactionRepository());
        } else {
            startRecover(transactionRepository);
        }
    }

    public void startRecover(TransactionRepository transactionRepository) {
        // 如果存储在内存，则使用默认的Lock；如果不是则获取默认的锁
        Lock recoveryLock = transactionRepository instanceof LocalStorable ? RecoveryLock.DEFAULT_LOCK : transactionConfigurator.getRecoveryLock();

        // 加锁
        if (recoveryLock.tryLock()) {
            try {
                String offset = null;
                int totalCount = 0;
                do {
                    // 获取异常事务列表
                    Page<Transaction> page = loadErrorTransactionsByPage(transactionRepository, offset);
                    if (page.getData().size() > 0) {
                        // 并发恢复异常事务
                        concurrentRecoveryErrorTransactions(transactionRepository, page.getData());
                        offset = page.getNextOffset();
                        totalCount += page.getData().size();
                    } else {
                        break;
                    }
                } while (true);

                logger.debug(String.format("total recovery count %d from repository:%s", totalCount, transactionRepository.getClass().getName()));
            } catch (Throwable e) {
                logger.error(String.format("recovery failed from repository:%s.", transactionRepository.getClass().getName()), e);
            } finally {
                recoveryLock.unlock();
            }
        }
    }

    private Page<Transaction> loadErrorTransactionsByPage(TransactionRepository transactionRepository, String offset) {

        long currentTimeInMillis = Instant.now().toEpochMilli();

        RecoverFrequency recoverFrequency = transactionConfigurator.getRecoverFrequency();
        //当前时间超过 - 事务变更时间( 最后执行时间 ) >= 事务恢复间隔( RecoverConfig#getRecoverDuration() )
        return transactionRepository.findAllUnmodifiedSince(new Date(currentTimeInMillis - recoverFrequency.getRecoverDuration() * 1000), offset, recoverFrequency.getFetchPageSize());
    }


    /**
     * 恢复异常事务集合
     * @param transactionRepository
     * @param transactions
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void concurrentRecoveryErrorTransactions(TransactionRepository transactionRepository, List<Transaction> transactions) throws InterruptedException, ExecutionException {

        initLogStatistics();

        List<RecoverTask> tasks = new ArrayList<>();
        for (Transaction transaction : transactions) {
            tasks.add(new RecoverTask(transactionRepository, transaction));
        }

        List<Future<Void>> futures = recoveryExecutorService.invokeAll(tasks, CONCURRENT_RECOVERY_TIMEOUT, TimeUnit.SECONDS);

        for (Future future : futures) {
            future.get();
        }
    }

    private void recoverErrorTransactions(TransactionRepository transactionRepository, List<Transaction> transactions) {

        initLogStatistics();

        for (Transaction transaction : transactions) {
            recoverErrorTransaction(transactionRepository, transaction);
        }
    }

    private void recoverErrorTransaction(TransactionRepository transactionRepository, Transaction transaction) {

        if (transaction.getRetriedCount() > transactionConfigurator.getRecoverFrequency().getMaxRetryCount()) {
            //当单个事务超过最大重试次数时，不再重试，只打印异常。
            logSync.lock();
            try {
                if (triggerMaxRetryPrintCount.get() < logMaxPrintCount) {
                    logger.error(String.format(
                            "recover failed with max retry count,will not try again. txid:%s, status:%s,retried count:%d,transaction content:%s",
                            transaction.getXid(),
                            transaction.getStatus().getId(),
                            transaction.getRetriedCount(),
                            JSON.toJSONString(transaction)));
                    triggerMaxRetryPrintCount.incrementAndGet();
                } else if (triggerMaxRetryPrintCount.get() == logMaxPrintCount) {
                    logger.error("Too many transaction's retried count max then MaxRetryCount during one page transactions recover process , will not print errors again!");
                }

            } finally {
                logSync.unlock();
            }

            return;
        }

        try {

            if (transaction.getTransactionType().equals(TransactionType.ROOT)) {
                // 如果是根事务
                switch (transaction.getStatus()) {
                    case CONFIRMING:
                        commitTransaction(transactionRepository, transaction);
                        break;
                    case CANCELLING:
                        rollbackTransaction(transactionRepository, transaction);
                        break;
                    default:
                        //the transaction status is TRYING, ignore it.
                        break;

                }

            } else {
                // 如果是分支事务
                //transaction type is BRANCH
                switch (transaction.getStatus()) {
                    case CONFIRMING:
                        commitTransaction(transactionRepository, transaction);
                        break;
                    case CANCELLING:
                    case TRY_FAILED:
                        rollbackTransaction(transactionRepository, transaction);
                        break;
                    case TRY_SUCCESS:

                        if(transactionRepository.getRootDomain() == null) {
                            break;
                        }

                        //check the root transaction
                        Transaction rootTransaction = transactionRepository.findByRootXid(transaction.getRootXid());

                        if (rootTransaction == null) {
                            // In this case means the root transaction is already rollback.
                            // Need cancel this branch transaction.
                            rollbackTransaction(transactionRepository, transaction);
                        } else {
                            switch (rootTransaction.getStatus()) {
                                case CONFIRMING:
                                    commitTransaction(transactionRepository, transaction);
                                    break;
                                case CANCELLING:
                                    rollbackTransaction(transactionRepository, transaction);
                                    break;
                                default:
                                    break;
                            }
                        }
                        break;
                    default:
                        // the transaction status is TRYING, ignore it.
                        break;
                }

            }

        } catch (Throwable throwable) {

            if (throwable instanceof TransactionOptimisticLockException
                    || ExceptionUtils.getRootCause(throwable) instanceof TransactionOptimisticLockException) {

                logger.warn(String.format(
                        "optimisticLockException happened while recover. txid:%s, status:%d,retried count:%d",
                        transaction.getXid(),
                        transaction.getStatus().getId(),
                        transaction.getRetriedCount()));
            } else {

                logSync.lock();
                try {
                    if (recoveryFailedPrintCount.get() < logMaxPrintCount) {
                        logger.error(String.format("recover failed, txid:%s, status:%s,retried count:%d,transaction content:%s",
                                transaction.getXid(),
                                transaction.getStatus().getId(),
                                transaction.getRetriedCount(),
                                JSON.toJSONString(transaction)), throwable);
                        recoveryFailedPrintCount.incrementAndGet();
                    } else if (recoveryFailedPrintCount.get() == logMaxPrintCount) {
                        logger.error("Too many transaction's recover error during one page transactions recover process , will not print errors again!");
                    }
                } finally {
                    logSync.unlock();
                }
            }
        }
    }

    private void rollbackTransaction(TransactionRepository transactionRepository, Transaction transaction) {
        transaction.setRetriedCount(transaction.getRetriedCount() + 1);
        transaction.setStatus(CANCELLING);
        transactionRepository.update(transaction);
        transaction.rollback();
        transactionRepository.delete(transaction);
    }

    private void commitTransaction(TransactionRepository transactionRepository, Transaction transaction) {
        transaction.setRetriedCount(transaction.getRetriedCount() + 1);
        transaction.setStatus(CONFIRMING);
        transactionRepository.update(transaction);
        transaction.commit();
        transactionRepository.delete(transaction);
    }


    private void ensureRecoveryInitialized() {

        if (recoveryExecutorService == null) {
            synchronized (TransactionRecovery.class) {
                if (recoveryExecutorService == null) {
                    // 创建线程池，用于执行任务
                    recoveryExecutorService = Executors.newFixedThreadPool(transactionConfigurator.getRecoverFrequency().getConcurrentRecoveryThreadCount());

                    logMaxPrintCount = transactionConfigurator.getRecoverFrequency().getFetchPageSize() / 2
                            > MAX_ERROR_COUNT_SHREDHOLD ?
                            MAX_ERROR_COUNT_SHREDHOLD : transactionConfigurator.getRecoverFrequency().getFetchPageSize() / 2;


                }
            }
        }
    }

    private void initLogStatistics() {
        triggerMaxRetryPrintCount.set(0);
        recoveryFailedPrintCount.set(0);
    }


    class RecoverTask implements Callable<Void> {

        TransactionRepository transactionRepository;
        Transaction transaction;

        public RecoverTask(TransactionRepository transactionRepository, Transaction transaction) {
            this.transactionRepository = transactionRepository;
            this.transaction = transaction;
        }

        @Override
        public Void call() throws Exception {
            recoverErrorTransaction(transactionRepository, transaction);
            return null;
        }
    }

}
