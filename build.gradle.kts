import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val defaultJdkVersion = 11
java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    idea
    `java-library`
    `jvm-test-suite`
    alias(libs.plugins.spotless)
    alias(libs.plugins.shadow)
    alias(libs.plugins.protobuf)
    id("java-test-fixtures")
}

group = "com.clickhouse.kafka"
version = file("VERSION").readText().trim()
description = "The official ClickHouse Apache Kafka Connect Connector."

repositories {
    mavenCentral()
    mavenLocal()
    maven("https://packages.confluent.io/maven/")
    maven("https://jitpack.io")
}

val clickhouseDependencies: Configuration by configurations.creating

dependencies {
    implementation(libs.kafka.connect.api)
    implementation(libs.clickhouse.client)
    implementation(libs.clickhouse.http.client)
    implementation(libs.clickhouse.data)
    implementation(libs.clickhouse.client.v2)
    implementation(libs.gson)
    implementation(libs.httpclient5)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(libs.jackson.core)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.annotations)

    /*
        Will in side the Confluent Archive
     */
    clickhouseDependencies(libs.httpclient5)
    clickhouseDependencies(libs.clickhouse.client)
    clickhouseDependencies(libs.clickhouse.client.v2)
    clickhouseDependencies(libs.clickhouse.http.client)
    clickhouseDependencies(libs.gson)
    clickhouseDependencies(libs.jackson.core)
    clickhouseDependencies(libs.jackson.databind)
    clickhouseDependencies(libs.jackson.annotations)

    // Unit Tests
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.platform.runner)
    testImplementation(libs.apiguardian.api)
    testImplementation(libs.hamcrest)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.clickhouse)
    testImplementation(libs.testcontainers.toxiproxy)

    // Schema Registry client for testing
    testImplementation(libs.kafka.schema.registry.client)
    testImplementation(libs.kafka.schema.registry)
    testImplementation(libs.kafka.schema.serializer)

    // Test Fixtures Dependencies
    // Test Fixtures is used to extract helpers into a separate jar and publish locally to use in other test projects
    // like JMH Benchmark
    // Protobuf dependencies
    testFixturesApi(libs.protobuf.java)
    testFixturesApi(libs.kafka.protobuf.serializer)
    testFixturesApi(libs.kafka.connect.protobuf.converter)
    testFixturesApi(libs.kafka.connect.avro.converter)
    testFixturesApi(libs.kafka.avro.serializer)

    testFixturesImplementation(platform(libs.junit.bom))
    testFixturesImplementation(libs.junit.jupiter)
    testFixturesImplementation(libs.junit.platform.runner)
    testFixturesImplementation(libs.kafka.connect.api)
    testFixturesImplementation(libs.commons.lang3)
    testFixturesImplementation(libs.clickhouse.client)
    testFixturesImplementation(libs.clickhouse.http.client)
    testFixturesImplementation(libs.clickhouse.data)
    testFixturesImplementation(libs.clickhouse.client.v2)
    testFixturesImplementation(libs.gson)
    testFixturesImplementation(libs.json)
    testFixturesImplementation(libs.testcontainers)
}

@Suppress("UnstableApiUsage")
testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter()
        }

        val integrationTest by registering(JvmTestSuite::class) {
            useJUnitJupiter()
            dependencies {
                implementation(project(":"))
                implementation(project.dependencies.testFixtures(project(":")))
                
                implementation(libs.testcontainers.clickhouse)
                implementation(libs.testcontainers.kafka)
                implementation(libs.testcontainers.toxiproxy)
                implementation(libs.okhttp)
                implementation(libs.json)
                implementation(libs.httpclient5.test)
                implementation(libs.clickhouse.jdbc)
                implementation(libs.clickhouse.client)
                implementation(libs.clickhouse.client.v2)
                implementation(libs.clickhouse.http.client)
                implementation(libs.slf4j.simple)
                implementation(libs.kafka.connect.api)
            }
            targets {
                all {
                    testTask.configure {
                        description = "Runs the integration tests"
                        group = "verification"
                        shouldRunAfter(test)
                        dependsOn("prepareConfluentArchive")
                        outputs.upToDateWhen { false }
                        
                        val systemProps = System.getProperties().entries.associate { it.key.toString() to it.value }
                        systemProperties(systemProps)
                    }
                }
            }
        }
    }
}

tasks.withType<JavaCompile> {
    options.compilerArgs.addAll(listOf("-Xlint:unchecked", "-Xlint:deprecation"))
    options.release.set(11)
}

tasks.withType<Test> {
    if (System.getenv("CLICKHOUSE_CLUSTER_NAME") != null) {
        maxParallelForks = 1
    } else {
        maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).takeIf { it > 0 } ?: 1
    }
    
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = TestExceptionFormat.FULL
    }
    
    val javaVersion: Int = (project.findProperty("javaVersion") as String? ?: defaultJdkVersion.toString()).toInt()
    logger.info("Running tests using JDK$javaVersion")

    systemProperties(mapOf("com.clickhouse.test.uri" to System.getProperty("com.clickhouse.test.uri", "")))
}

/*
 * ShadowJar
 */
tasks.withType<Jar> {
    manifest {
        attributes["Implementation-Title"] = "ClickHouse-Kafka-Connect"
        attributes["Implementation-Version"] = project.version.toString()
    }
}
tasks.register<ShadowJar>("confluentJar") {
    archiveClassifier.set("confluent")
    from(clickhouseDependencies, sourceSets.main.get().output)
}

// Confluent Archive
val releaseDate by extra(DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDateTime.now()))
val archiveFilename = "clickhouse-kafka-connect"
tasks.register<Copy>("prepareConfluentArchive") {
    group = "Confluent"
    description = "Prepares the Confluent Archive ready for the hub"
    dependsOn("confluentJar")

    val baseDir = "$archiveFilename-${project.version}"
    destinationDir = layout.buildDirectory.dir("confluentArchive/$baseDir").get().asFile

    from("config/archive/manifest.json") {
        expand(project.properties)
    }

    from("config/archive/assets") {
        into("assets")
    }

    from(layout.buildDirectory.dir("libs")) {
        include("${project.name}-${project.version}-confluent.jar")
        into("lib")
    }

    from(".") {
        include("README.md", "LICENSE")
        into("doc")
    }
}

tasks.register<Zip>("createConfluentArchive") {
    group = "Confluent"
    description = "Creates the Confluent Archive zipfile to be uploaded to the Confluent Hub"
    dependsOn("prepareConfluentArchive")
    from(layout.buildDirectory.dir("confluentArchive"))
    archiveBaseName.set("")
    archiveAppendix.set(archiveFilename)
    archiveVersion.set(project.version.toString())
    destinationDirectory.set(layout.buildDirectory.dir("confluent"))
}

var generateVersionFile = tasks.register<DefaultTask>("generateVersionFile") {
    val outputDir = "generated/sources/version/java/main/com/clickhouse/kafka/connect/sink/";
    outputs.dir(layout.buildDirectory.dir(outputDir))
    doFirst {
        val outputDirFile = layout.buildDirectory.dir(outputDir).get().asFile
        outputDirFile.mkdirs() // Ensure the directory exists

        val outputFile = File(outputDirFile, "Version.java")
        val versionContent = """
package com.clickhouse.kafka.connect.sink;

public final class Version {
    public static final String ARTIFACT_VERSION = "${project.version}";

    private Version() {
        // Prevent instantiation
    }
}
""".trimIndent()

        outputFile.writeText(versionContent)
    }
}
tasks.named("compileJava") {
    dependsOn("generateVersionFile")
}

sourceSets {
    main {
        java {
            srcDir(generateVersionFile)
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.libprotobuf.get()}"
    }
}