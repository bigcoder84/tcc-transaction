package org.mengyun.tcctransaction.common;

/**
 * Created by changmingxie on 11/11/15.
 */
public enum ParticipantRole {
    ROOT,
    CONSUMER,
    PROVIDER,
    /**
     * 不进行事务处理
     */
    NORMAL;
}
