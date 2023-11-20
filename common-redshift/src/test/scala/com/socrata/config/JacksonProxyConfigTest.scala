package com.socrata.config

import org.junit.jupiter.api.{DisplayName, Test}


@DisplayName("Jackson Proxy Config Tests")
class JacksonProxyConfigTest {

  @DisplayName("Nested traits")
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

  @DisplayName("List")
  @Test
  def list(): Unit = {

    trait Recipe {
      def ingredients(): List[String]
    }

    val recipe: Recipe =
      JacksonProxyConfigBuilder(CommonObjectMapperCustomizer.Default)
        .withSources(JacksonYamlConfigSource("data/recipe.yaml"))
        .proxy( classOf[Recipe])

    assert(List("Tomato","Cheese","Bread").equals(recipe.ingredients()))
  }

  @DisplayName("List of complex type")
  @Test
  def listComplex(): Unit = {

    trait Ingredient{
      def name(): String
      def amount(): Int
    }

    trait Recipe {
      def ingredients(): List[Ingredient]
    }

    val recipe: Recipe =
      JacksonProxyConfigBuilder(CommonObjectMapperCustomizer.Default)
        .withSources(JacksonYamlConfigSource("data/recipe2.yaml"))
        .proxy(classOf[Recipe])

    assert(
      List(
        Map("name" -> "Tomato", "amount" -> 3),
        Map("name" -> "Cheese", "amount" -> 32),
        Map("name" -> "Bread", "amount" -> 1)
      ).equals(recipe.ingredients())
    )
  }

}
