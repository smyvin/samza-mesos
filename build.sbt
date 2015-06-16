import com.banno._

name := "samza-mesos"

BannoSettings.settings

libraryDependencies ++= {
  val samzaVersion = "0.9.0"
  val mesosVersion = "0.21.0"
  Seq(
    "org.apache.samza" % "samza-api" % samzaVersion,
    "org.apache.samza" %% "samza-core" % samzaVersion,
    "org.apache.mesos" % "mesos" % mesosVersion,
    "org.scalatest" %% "scalatest" % "2.2.2" % "test"
  )
}
