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

  @DisplayName("List of super duper complex types")
  @Test
  def listSuperComplex(): Unit = {

    trait Name{
      def short(): String
      def long(): String
    }

    trait Ingredient {
      def name(): Name

      def amount(): Int
    }

    trait Recipe {
      def ingredients(): List[Ingredient]
    }

    val recipe: Recipe =
      JacksonProxyConfigBuilder(CommonObjectMapperCustomizer.Default)
        .withSources(JacksonYamlConfigSource("data/recipe3.yaml"))
        .proxy(classOf[Recipe])

    val ingredients:List[Ingredient] = recipe.ingredients()
    val tomato:Ingredient = ingredients.filter(_.name().long().equals("Tomato")).head
    val cheese:Ingredient = ingredients.filter(_.name().long().equals("Cheese")).head
    val Bread:Ingredient = ingredients.filter(_.name().long().equals("Bread")).head
    assert(3.equals(tomato.amount()))
    val tomatoName:Name = tomato.name()
    assert("Tomato".equals(tomatoName.long()))
    assert("T".equals(tomatoName.short()))
  }

}
