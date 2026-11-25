package com.fortune.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.ClassPathResource;

/**
 * application.yml 의 fortune.* 키가 FortuneProperties 에 실제로 바인딩되는지 검증한다.
 * (오타가 있으면 Spring 은 조용히 무시하므로, 키 이름 회귀를 여기서 잡는다.)
 */
class ConfigBindingTest {

    private FortuneProperties bind() throws Exception {
        MutablePropertySources sources = new MutablePropertySources();
        List<org.springframework.core.env.PropertySource<?>> loaded =
                new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"));
        loaded.forEach(sources::addLast);
        Binder binder = new Binder(
                ConfigurationPropertySources.from(sources),
                new PropertySourcesPlaceholdersResolver(sources));
        return binder.bind("fortune", Bindable.of(FortuneProperties.class)).get();
    }

    @Test
    @DisplayName("결제/레이트리밋/아웃박스/카프카 설정이 yml 값으로 바인딩된다")
    void bindsFortuneProperties() throws Exception {
        FortuneProperties properties = bind();

        assertThat(properties.getIdempotency().getLockTtl()).isEqualTo(Duration.ofSeconds(10));
        assertThat(properties.getIdempotency().getRecordTtl()).isEqualTo(Duration.ofHours(24));
        assertThat(properties.getIdempotency().getKeyMaxLength()).isEqualTo(128);

        assertThat(properties.getRateLimit().isEnabled()).isTrue();
        assertThat(properties.getRateLimit().getRequestsPerWindow()).isEqualTo(100);
        assertThat(properties.getRateLimit().getWindow()).isEqualTo(Duration.ofSeconds(1));
        assertThat(properties.getRateLimit().isFailOpen()).isTrue();
        assertThat(properties.getRateLimit().getPathPrefixes()).containsExactly("/api/v1");

        assertThat(properties.getKafka().getTopic()).isEqualTo("fortune.paid");
        assertThat(properties.getKafka().getDlqTopic()).isEqualTo("fortune.paid.dlq");
        assertThat(properties.getKafka().getPartitions()).isEqualTo(3);

        assertThat(properties.getOutbox().getBatchSize()).isEqualTo(50);
        assertThat(properties.getOutbox().getMaxAttempts()).isEqualTo(5);
        assertThat(properties.getReading().getStatusTtl()).isEqualTo(Duration.ofDays(7));
        assertThat(properties.getInternal().getToken()).isEqualTo("local-internal-token");
    }
}
