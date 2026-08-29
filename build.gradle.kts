plugins {
    `java-library`
}

group = "me.xkyrell"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly(libs.jetbrains.annotations)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.junit)
    testRuntimeOnly(libs.junit.launcher)
}

tasks {
    compileJava {
        options.encoding = Charsets.UTF_8.name()
    }

    test {
        useJUnitPlatform()
    }
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}