package org.javi.master.shared.utils.mongo

case class MongoConfig (
                      inputUri: Option[String],
                      outputUri: Option[String],
                      inputDb: Option[String],
                      outputDb: Option[String],
                      inputCollection: Option[String],
                      outputCollection: Option[String]

                      )
