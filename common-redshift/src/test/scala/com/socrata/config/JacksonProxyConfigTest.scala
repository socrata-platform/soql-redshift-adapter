package com.socrata.config

import org.junit.jupiter.api.{DisplayName, Test}


@DisplayName("Jackson Proxy Config Tests")
class JacksonProxyConfigTest {

  @DisplayName("Person sanity test")
  @Test
  def person():Unit={

    trait Address {
      def country(): String
      def state(): String
      def city(): String
    }

    trait Person {
      def name(): String
      def age(): Integer
      def address(): Address
    }

    val person: Person =
      JacksonProxyConfigBuilder(CommonObjectMapperCustomizer.Default)
        .withSources(JacksonYamlConfigSource("data/person.yaml"))
        .proxy("person", classOf[Person])

    assert("John".equals(person.name()))
    assert(20.equals(person.age()))
    assert("United States".equals(person.address().country()))
    assert("New York".equals(person.address().state()))
    assert("Buffalo".equals(person.address().city()))
  }

}
