package com.socrata.controller

import com.socrata.api.PetsApi
import com.socrata.beans.{NewPet, Pet}
import jakarta.enterprise.context.ApplicationScoped

import java.{lang, util}

@ApplicationScoped
class Pets extends PetsApi {
  override def addPet(newPet: NewPet): Pet = ???

  override def deletePet(id: lang.Long): Unit = ???

  override def findPetById(id: lang.Long): Pet = {

    Pet.builder()
      .id(1)
      .name("Jackson%s".format(id))
      .tag("something")
      .build()
  }

  override def findPets(tags: util.List[String], limit: Integer): util.List[Pet] = {
    val pet = new Pet();
    pet.name("Rex");
    pet.id(3);
    pet.tag("something");
    util.List.of(
      Pet.builder()
        .id(1)
        .name("Jackson")
        .tag("something")
        .build(),
      new Pet("Ari", "something tag", 2),
      pet,
      pet.withId(4).name("Alex"),
      new Pet().id(5).name("Dexter").tag("something")
    )
  }
}
