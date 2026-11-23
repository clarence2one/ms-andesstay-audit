package com.andesstay.ms_andesstay_audit.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Topicos del caso AndesStay (seccion 9). reservations.events es la fuente de verdad
 * publicada por ms-andesstay-reservations; audit.timeline es el historial normalizado
 * que este servicio republica tras persistir cada evento. El *.DLT de reservations.events
 * lo crea automaticamente el DeadLetterPublishingRecoverer (ver KafkaConsumerConfig) la
 * primera vez que un mensaje agota sus reintentos, pero declararlo aqui asegura que ya
 * exista con la configuracion correcta desde el arranque.
 */
@Configuration
public class KafkaTopicsConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaTopicsConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public NewTopic reservationsEventsTopic() {
        return topic(kafkaProperties.topicReservationsEvents(),
                kafkaProperties.reservationsRetentionMs(), kafkaProperties.reservationsRetentionBytes());
    }

    @Bean
    public NewTopic reservationsEventsDeadLetterTopic() {
        return topic(kafkaProperties.topicReservationsEvents() + ".DLT",
                kafkaProperties.dltRetentionMs(), kafkaProperties.dltRetentionBytes());
    }

    @Bean
    public NewTopic auditTimelineTopic() {
        return topic(kafkaProperties.topicAuditTimeline(),
                kafkaProperties.auditRetentionMs(), kafkaProperties.auditRetentionBytes());
    }

    /**
     * Cada flujo tiene su propia política de retención (materia 3.3.1): se aplica el
     * criterio que ocurra primero, por tiempo o por tamaño. min.insync.replicas=2
     * acompaña a acks=all del productor para no confirmar escrituras solo en el líder.
     */
    private NewTopic topic(String name, long retentionMs, long retentionBytes) {
        return new NewTopic(name, kafkaProperties.partitions(), (short) kafkaProperties.replicationFactor())
                .configs(Map.of(
                        "retention.ms", Long.toString(retentionMs),
                        "retention.bytes", Long.toString(retentionBytes),
                        "cleanup.policy", kafkaProperties.cleanupPolicy(),
                        "min.insync.replicas", Integer.toString(kafkaProperties.minInsyncReplicas())
                ));
    }
}
