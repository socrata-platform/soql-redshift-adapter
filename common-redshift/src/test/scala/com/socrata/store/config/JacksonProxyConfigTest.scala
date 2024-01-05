package com.socrata.config

import com.socrata.common.config.{CommonObjectMapperCustomizer, JacksonProxyConfigBuilder, JacksonYamlConfigSource}
import org.junit.jupiter.api.{DisplayName, Test}

import java.io.File
import java.util.Properties

@DisplayName("Jackson Proxy Config Tests")
class JacksonProxyConfigTest {

  @DisplayName("Nested traits")
  @Test
  def person(): Unit = {

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
        .proxy(classOf[Recipe])

    assert(List("Tomato", "Cheese", "Bread").equals(recipe.ingredients()))
  }

  @DisplayName("List of complex type")
  @Test
  def listComplex(): Unit = {

    trait Ingredient {
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

    val ingredients: List[Ingredient] = recipe.ingredients()
    val tomato: Ingredient = ingredients.filter(_.name().equals("Tomato")).head
    val cheese: Ingredient = ingredients.filter(_.name().equals("Cheese")).head
    val bread: Ingredient = ingredients.filter(_.name().equals("Bread")).head
    assert(3.equals(tomato.amount()))
    assert(32.equals(cheese.amount()))
    assert(1.equals(bread.amount()))
  }

  @DisplayName("List of super duper complex types")
  @Test
  def listSuperComplex(): Unit = {

    trait Name {
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

    val ingredients: List[Ingredient] = recipe.ingredients()
    val tomato: Ingredient = ingredients.filter(_.name().long().equals("Tomato")).head
    val cheese: Ingredient = ingredients.filter(_.name().long().equals("Cheese")).head
    val bread: Ingredient = ingredients.filter(_.name().long().equals("Bread")).head
    assert(3.equals(tomato.amount()))
    val tomatoName: Name = tomato.name()
    assert("Tomato".equals(tomatoName.long()))
    assert("T".equals(tomatoName.short()))

    assert(32.equals(cheese.amount()))
    val cheeseName: Name = cheese.name()
    assert("Cheese".equals(cheeseName.long()))
    assert("C".equals(cheeseName.short()))

    assert(1.equals(bread.amount()))
    val breadName: Name = bread.name()
    assert("Bread".equals(breadName.long()))
    assert("B".equals(breadName.short()))
  }

  @DisplayName("Optional")
  @Test
  def optional(): Unit = {

    trait Dog {
      def name(): String
    }

    trait Person {
      def dog(): Option[Dog]
    }

    val person: Person =
      JacksonProxyConfigBuilder(CommonObjectMapperCustomizer.Default)
        .withSources(JacksonYamlConfigSource("data/person-dog.yaml"))
        .proxy("person", classOf[Person])

    assert("Jackson".equals(person.dog().get.name()))
  }

  @DisplayName("Interpolation")
  @Test
  def interpolation(): Unit = {

    trait Server {
      def dataDir(): File
    }

    val server: Server =
      JacksonProxyConfigBuilder(CommonObjectMapperCustomizer.Default)
        .withSources(JacksonYamlConfigSource("data/server.yaml"))
        .proxy("server", classOf[Server])

    assert(server.dataDir() != null)
  }

  @DisplayName("Environment")
  @Test
  def environment(): Unit = {

    trait Resources {
      def cpu(): Int
      def gpu(): Int
      def storage(): Int
    }

    val resources: Resources =
      JacksonProxyConfigBuilder(CommonObjectMapperCustomizer.Default)
        .withSources(JacksonYamlConfigSource("data/resources.yaml"))
        .withEnvs(() => {
          new Properties() {
            {
              setProperty("CPU_AMOUNT", "1")
              setProperty("GPU_AMOUNT", "2")
              setProperty("STORAGE_AMOUNT", "3")
            }
          }
        })
        .proxy(classOf[Resources])

    assert(1.equals(resources.cpu()))
    assert(2.equals(resources.gpu()))
    assert(3.equals(resources.storage()))
  }

}
