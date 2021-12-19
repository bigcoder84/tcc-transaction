package org.mengyun.tcctransaction.recovery;


/**
 * Created by changming.xie on 6/1/16.
 */
public interface RecoverFrequency {

    /**
     * 单个事务恢复最大重试次数。超过最大重试次数后，打出错误日志。
     */
    int getMaxRetryCount();

    int getFetchPageSize();

    /**
     * 恢复间隔时间
     * @return
     */
    int getRecoverDuration();

    /**
     * cron 表达式
     * @return
     */
    String getCronExpression();

    int getConcurrentRecoveryThreadCount();
}
