package com.fortune.core.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RequestHasherTest {

    @Test
    @DisplayName("같은 본문은 항상 같은 지문이 된다")
    void deterministic() {
        String first = RequestHasher.sha256Hex("order|4900|KRW");
        String second = RequestHasher.sha256Hex("order|4900|KRW");

        assertThat(first).isEqualTo(second);
        assertThat(first).hasSize(64);
    }

    @Test
    @DisplayName("본문이 다르면 지문도 달라진다")
    void differentPayloads() {
        assertThat(RequestHasher.sha256Hex("order|4900|KRW"))
                .isNotEqualTo(RequestHasher.sha256Hex("order|4800|KRW"));
    }
}
