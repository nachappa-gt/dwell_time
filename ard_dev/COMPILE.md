# Compiling User Frequency Summary.

## S3 Maven Repository

This projects depends on the spring-common module in the S3 Maven repository.
Do to following to use this repository:

1. Create "project/plugins.sbt" with the following line:

   addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.9.0")


2. Create ~/.sbt/.s3credentials with the following lines:

    accessKey = XXXXXXXXXX
    secretKey = XXXXXXXXXX


Reference:
  - https://github.com/frugalmechanic/fm-sbt-s3-resolver
