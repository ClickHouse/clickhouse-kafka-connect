/*
 * The settings file is used to specify which projects to include in your build.
 * For more detailed information on multi-project builds, please refer to https://docs.gradle.org/8.14.2/userguide/multi_project_builds.html in the Gradle documentation.
 */

rootProject.name = "kafka-connector-benchmark"

/*
 * This is a standalone build. It does NOT use a Gradle composite build to consume
 * the connector from the parent project. Instead, it depends on the published
 * connector artifact by version, resolved from the local Maven repository (~/.m2).
 *
 * Before running the benchmark, publish the connector locally from the repo root:
 *   ./gradlew publishToMavenLocal
 */

// Reuse the connector's version catalog so the few libraries the benchmark has to
// declare itself stay pinned to the versions the connector was built against.
dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            from(files("../gradle/libs.versions.toml"))
        }
    }
}
