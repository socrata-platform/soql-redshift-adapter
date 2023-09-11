package com.socrata.impl

import com.socrata.api.PetsApi
import com.socrata.beans.{NewPet, Pet}
import jakarta.enterprise.context.ApplicationScoped

import java.{lang, util}

@ApplicationScoped
class Pets extends PetsApi{
  override def addPet(newPet: NewPet): Pet = ???

  override def deletePet(id: lang.Long): Unit = ???

  override def findPetById(id: lang.Long): Pet = ???

  override def findPets(tags: util.List[String], limit: Integer): util.List[Pet] = ???
}
