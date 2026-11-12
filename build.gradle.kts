plugins {
    java
    id("org.springframework.boot") version "4.1.2"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "lab"
version = "0.1.0"

java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }

repositories { mavenCentral() }

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework:spring-jdbc")
    implementation("com.zaxxer:HikariCP")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.20.2")
    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation(platform("org.testcontainers:testcontainers-bom:2.0.5"))
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.awaitility:awaitility")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Main suite: everything except the performance budget, test classes in parallel.
tasks.test {
    useJUnitPlatform { excludeTags("perf", "isolated") }
    testLogging { events("passed", "failed"); exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
}

// The lag budget is measured alone, after the main suite, without competing load.
val perfTest by tasks.registering(Test::class) {
    description = "Consumer lag budget test, run alone."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("perf") }
    shouldRunAfter(tasks.test)
    testLogging { events("passed", "failed"); showStandardStreams = true }
}
// Docker-level fault injection (pausing a broker) runs alone too, after the main suite.
val isolatedTest by tasks.registering(Test::class) {
    description = "Tests that manipulate Docker (broker outage), run alone."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("isolated") }
    shouldRunAfter(tasks.test)
    // ProcessKillTest starts the ledger service as a separate JVM with this classpath.
    systemProperty("lab.serviceClasspath", sourceSets.main.get().runtimeClasspath.asPath)
    testLogging { events("passed", "failed") }
}
perfTest { shouldRunAfter(isolatedTest) }
tasks.check { dependsOn(isolatedTest, perfTest) }
