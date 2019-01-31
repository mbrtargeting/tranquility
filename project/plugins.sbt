resolvers ++= Seq(
  "Central" at "https://oss.sonatype.org/content/repositories/releases/"
)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "latest.release")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "latest.release")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "latest.release")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "latest.release")
