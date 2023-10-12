package com.socrata.config

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.redshift.{AmazonRedshift, AmazonRedshiftClientBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import jakarta.inject.Named
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
class AwsConfig {

  @Produces
  def awsCredentials
  (
    @ConfigProperty(name = "aws.accessKey") accessKey: String,
    @ConfigProperty(name = "aws.secretKey") secretKey: String
  ): AWSCredentials={
    new BasicAWSCredentials(accessKey, secretKey)
  }

  @Produces
  def awsCredentialsProvider
  (
    credentials: AWSCredentials
  ): AWSCredentialsProvider = {
    new AWSStaticCredentialsProvider(credentials)
  }

  @Produces
  def awsS3
  (
    credentialsProvider: AWSCredentialsProvider,
    @ConfigProperty(name = "aws.region") region: String
  ): AmazonS3 = {
    AmazonS3ClientBuilder.standard()
      .withRegion(Regions.fromName(region))
      .withCredentials(credentialsProvider)
      .build()
  }

  @Produces
  def awsRedshift
  (
    credentialsProvider: AWSCredentialsProvider,
    @ConfigProperty(name = "aws.region") region: String
  ):AmazonRedshift={
    AmazonRedshiftClientBuilder.standard()
      .withCredentials(credentialsProvider)
      .withRegion(region)
      .build();
  }

}
