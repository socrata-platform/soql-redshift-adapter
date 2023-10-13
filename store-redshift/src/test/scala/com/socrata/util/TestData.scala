package com.socrata.util

import com.opencsv.{CSVParserBuilder, CSVReaderBuilder}

import java.io.{File, FileReader}

object TestData {

  def readTestData(path: String) = {
    val reader = new FileReader(new File(getClass.getResource(path).toURI))

    val parser = new CSVParserBuilder().withSeparator(',')
      .withIgnoreQuotations(false)
      .withIgnoreLeadingWhiteSpace(true)
      .withStrictQuotes(false)
      .build();

    val csvReader = new CSVReaderBuilder(reader)
      .withSkipLines(1)
      .withCSVParser(parser)
      .build();
    csvReader
  }

}
