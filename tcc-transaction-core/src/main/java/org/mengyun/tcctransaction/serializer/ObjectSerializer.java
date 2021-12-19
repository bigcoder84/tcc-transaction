package org.mengyun.tcctransaction.serializer;

/**
 * 对象序列化器
 * Transaction 是一个比较复杂的对象，内嵌 Participant 数组，而 Participant 本身也是复杂的对象，内嵌了更多的其他对象，
 * 因此，存储器在持久化 Transaction 时，需要序列化后才能存储。
 * Created by changming.xie on 7/22/16.
 */
public interface ObjectSerializer<T> {

    /**
     * Serialize the given object to binary data.
     *
     * @param t object to serialize
     * @return the equivalent binary data
     */
    byte[] serialize(T t);

    /**
     * Deserialize an object from the given binary data.
     *
     * @param bytes object binary representation
     * @return the equivalent object instance
     */
    T deserialize(byte[] bytes);


    T clone(T object);
}
