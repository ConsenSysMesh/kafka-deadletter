package net.consensys.kafkadl.internal.beanregistration;

import lombok.Data;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.FactoryBean;

@Data
public class TopicBeanFactory implements FactoryBean<NewTopic> {

    private String topicName;

    private String topicSuffix;

    public TopicBeanFactory() {
    }

    @Override
    public NewTopic getObject() throws Exception {
        return new NewTopic(getFullTopicName(), 3, Short.parseShort("1"));
    }

    @Override
    public Class<?> getObjectType() {
        return NewTopic.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    private String getFullTopicName() {
        return getTopicName() + getTopicSuffix();
    }
}
