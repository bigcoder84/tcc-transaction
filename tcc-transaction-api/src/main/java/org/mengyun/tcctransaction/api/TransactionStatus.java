package org.mengyun.tcctransaction.api;

/**
 * Created by changmingxie on 10/28/15.
 */
public enum TransactionStatus {

    /**
     * 尝试中
     */
    TRYING(1),
    /**
     * 确认中
     */
    CONFIRMING(2),
    /**
     * 取消中状态
     */
    CANCELLING(3),
    /**
     * 尝试成功
     */
    TRY_SUCCESS(11),
    /**
     * 尝试失败
     */
    TRY_FAILED(12);

    private int id;

    TransactionStatus(int id) {
        this.id = id;
    }

    public static TransactionStatus valueOf(int id) {

        switch (id) {
            case 1:
                return TRYING;
            case 2:
                return CONFIRMING;
            case 3:
                return CANCELLING;
            case 11:
                return TRY_SUCCESS;
            case 12:
                return TRY_FAILED;
            default:
                throw new IllegalArgumentException("the id "+id+" of TransactionStatus is illegal.");
        }
    }

    public int getId() {
        return id;
    }

}
