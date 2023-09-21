/*
 * This file was generated by the Gradle 'init' task.
 *
 * This is a general purpose Gradle build.
 * Learn more about Gradle by exploring our samples at https://docs.gradle.org/7.4.2/samples
 * This project uses @Incubating APIs which are subject to change.
 */

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import java.io.ByteArrayOutputStream
import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val defaultJdkVersion = 17
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    idea
    `java-library`
    `maven-publish`
    signing
   // checkstyle
    id("com.github.gmazzo.buildconfig") version "3.0.3"
    //id("com.github.spotbugs") version "4.7.9"
    id("com.diffplug.spotless") version "5.17.1"
    id("com.github.johnrengelman.shadow") version "6.1.0"
}

group = "com.clickhouse.kafka"
version = file("VERSION").readText().trim()
description = "The official ClickHouse Apache Kafka Connect Connector."

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
    maven("https://jitpack.io")
}

extra.apply {

    set("clickHouseDriverVersion", "0.4.6")
    set("kafkaVersion", "2.7.0")
    set("avroVersion", "1.9.2")

    // Testing dependencies
    set("junitJupiterVersion", "5.9.2")
    set("junitPlatformVersion", "1.8.1")
    set("hamcrestVersion", "2.2")
    set("mockitoVersion", "4.0.0")

    // Integration test dependencies
    set("confluentVersion", "6.0.1")
    set("scalaVersion", "2.13")
    set("curatorVersion", "2.9.0")
    set("connectUtilsVersion", "0.4+")
}

val clickhouseDependencies: Configuration by configurations.creating

dependencies {
    implementation("org.apache.kafka:connect-api:${project.extra["kafkaVersion"]}")
    implementation("com.clickhouse:clickhouse-client:${project.extra["clickHouseDriverVersion"]}")
    implementation("com.clickhouse:clickhouse-http-client:${project.extra["clickHouseDriverVersion"]}")
    implementation("com.clickhouse:clickhouse-data:${project.extra["clickHouseDriverVersion"]}")
    implementation("io.lettuce:lettuce-core:6.2.0.RELEASE")
    implementation("com.google.code.gson:gson:2.10")

    // TODO: need to remove ???
    implementation("org.slf4j:slf4j-reload4j:1.7.36")
    implementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
    implementation("org.testcontainers:testcontainers:1.19.0")
    implementation("org.testcontainers:toxiproxy:1.19.0")

    /*
        Will in side the Confluent Archive
     */
    clickhouseDependencies("io.lettuce:lettuce-core:6.2.0.RELEASE")
    clickhouseDependencies("com.clickhouse:clickhouse-client:${project.extra["clickHouseDriverVersion"]}")
    clickhouseDependencies("com.clickhouse:clickhouse-http-client:${project.extra["clickHouseDriverVersion"]}")
    clickhouseDependencies("com.google.code.gson:gson:2.10")

    // Unit Tests
    testImplementation(platform("org.junit:junit-bom:${project.extra["junitJupiterVersion"]}"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.platform:junit-platform-runner")
    testImplementation("org.apiguardian:apiguardian-api:1.1.2") // https://github.com/gradle/gradle/issues/18627
    testImplementation("org.hamcrest:hamcrest:${project.extra["hamcrestVersion"]}")
    testImplementation("org.mockito:mockito-junit-jupiter:${project.extra["mockitoVersion"]}")

    // IntegrationTests
    testImplementation("org.testcontainers:clickhouse:1.19.0")
    testImplementation("org.testcontainers:kafka:1.19.0")
    testImplementation("com.clickhouse:clickhouse-jdbc:0.4.6:all")
    testImplementation("com.squareup.okhttp3:okhttp:4.11.0")
    testImplementation("org.json:json:20230227")
    testImplementation("org.testcontainers:toxiproxy:1.19.0")

}


sourceSets.create("integrationTest") {
    java.srcDir("src/integrationTest/java")
    compileClasspath += sourceSets["main"].output + configurations["testRuntimeClasspath"]
    runtimeClasspath += output + compileClasspath + sourceSets["test"].runtimeClasspath
}
tasks.create("integrationTest", Test::class.java) {
    description = "Runs the integration tests"
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    outputs.upToDateWhen { false }
    dependsOn("prepareConfluentArchive")
    mustRunAfter("test")
}


sourceSets.create("exactlyOnceTest") {
    java.srcDir("src/exactlyOnceTest/java")
    compileClasspath += sourceSets["main"].output + configurations["testRuntimeClasspath"]
    runtimeClasspath += output + compileClasspath + sourceSets["test"].runtimeClasspath
}
tasks.create("exactlyOnceTest", Test::class.java) {
    description = "Runs the exactly once tests"
    group = "verification"
    testClassesDirs = sourceSets["exactlyOnceTest"].output.classesDirs
    classpath = sourceSets["exactlyOnceTest"].runtimeClasspath
    outputs.upToDateWhen { false }
    dependsOn("prepareConfluentArchive")
    mustRunAfter("test")
    testLogging.showStandardStreams = true
}

tasks.withType<Test> {
    tasks.getByName("check").dependsOn(this)
    systemProperty("file.encoding", "windows-1252") // run tests with different encoding
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }

    val javaVersion: Int = (project.findProperty("javaVersion") as String? ?: defaultJdkVersion.toString()).toInt()
    logger.info("Running tests using JDK$javaVersion")
    javaLauncher.set(javaToolchains.launcherFor {
        languageVersion.set(JavaLanguageVersion.of(javaVersion))
    })

    systemProperties(mapOf("com.clickhouse.test.uri" to System.getProperty("com.clickhouse.test.uri", "")))

    val jdkHome = project.findProperty("jdkHome") as String?
    jdkHome.let {
        val javaExecutablesPath = File(jdkHome, "bin/java")
        if (javaExecutablesPath.exists()) {
            executable = javaExecutablesPath.absolutePath
        }
    }

    addTestListener(object : TestListener {
        override fun beforeTest(testDescriptor: TestDescriptor?) {}
        override fun beforeSuite(suite: TestDescriptor?) {}
        override fun afterTest(testDescriptor: TestDescriptor?, result: TestResult?) {}
        override fun afterSuite(d: TestDescriptor?, r: TestResult?) {
            if (d != null && r != null && d.parent == null) {
                val resultsSummary = """Tests summary:
                    | ${r.testCount} tests,
                    | ${r.successfulTestCount} succeeded,
                    | ${r.failedTestCount} failed,
                    | ${r.skippedTestCount} skipped""".trimMargin().replace("\n", "")

                val border = "=".repeat(resultsSummary.length)
                logger.lifecycle("\n$border")
                logger.lifecycle("Test result: ${r.resultType}")
                logger.lifecycle(resultsSummary)
                logger.lifecycle("${border}\n")
            }
        }
    })
}

/*
 * ShadowJar
 */
tasks.register<ShadowJar>("confluentJar") {
    archiveClassifier.set("confluent")
    from(clickhouseDependencies, sourceSets.main.get().output)
}

/*
tasks.register<ShadowJar>("allJar") {
    archiveClassifier.set("all")
    from(clickhouseDependencies, sourceSets.main.get().output)
}
*/

// Confluent Archive
val releaseDate by extra(DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDateTime.now()))
val archiveFilename = "clickhouse-kafka-connect"
tasks.register<Copy>("prepareConfluentArchive") {
    group = "Confluent"
    description = "Prepares the Confluent Archive ready for the hub"
    dependsOn("confluentJar")

    val baseDir = "$archiveFilename-${project.version}"
    from("config/archive/manifest.json") {
        expand(project.properties)
        destinationDir = file("$buildDir/confluentArchive/$baseDir")
    }

    from("config/archive/assets") {
        into("assets")
    }

    from("$buildDir/libs") {
        include(listOf("${project.name}-${project.version}-confluent.jar"))
        into("lib")
    }

    from(".") {
        include(listOf("README.md", "LICENSE"))
        into("doc")
    }
}

tasks.register<Zip>("createConfluentArchive") {
    group = "Confluent"
    description = "Creates the Confluent Archive zipfile to be uploaded to the Confluent Hub"
    dependsOn("prepareConfluentArchive")
    from(files("$buildDir/confluentArchive"))
    archiveBaseName.set("")
    archiveAppendix.set(archiveFilename)
    archiveVersion.set(project.version.toString())
    destinationDirectory.set(file("$buildDir/confluent"))
}

tasks.getByName("integrationTest") {
    onlyIf{ System.getenv("HOST") != null &&  System.getenv("PORT") != null && System.getenv("PASSWORD") != null}
}
