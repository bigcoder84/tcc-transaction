package org.mengyun.tcctransaction.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by changmingxie on 10/25/15.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Compensable {

    /**
     * 事务传播级别
     * @return
     */
    public Propagation propagation() default Propagation.REQUIRED;

    /**
     * confirm执行的方法名称
     * @return
     */
    public String confirmMethod() default "";

    /**
     * cancel执行的方法名称
     * @return
     */
    public String cancelMethod() default "";

    /**
     * 是否异步执行confirm
     * @return
     */
    public boolean asyncConfirm() default false;

    /**
     * 是否异步执行cancel
     * @return
     */
    public boolean asyncCancel() default false;

    /**
     * 事务上下文编辑器 默认实现是：DefaultTransactionContextEditor
     * @return
     */
    public Class<? extends TransactionContextEditor> transactionContextEditor() default NullableTransactionContextEditor.class;
}