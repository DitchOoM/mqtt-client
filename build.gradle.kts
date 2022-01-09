plugins {
    kotlin("multiplatform") version "1.6.10"
    id("com.android.library")
    id("io.codearte.nexus-staging") version "0.30.0"
    `maven-publish`
    signing
}

val libraryVersionPrefix: String by project
group = "com.ditchoom"
version = "$libraryVersionPrefix.0-SNAPSHOT"

repositories {
    google()
    mavenCentral()
}

kotlin {
    android {
        publishLibraryVariants("release")
    }
    jvm {
        compilations.all {
            kotlinOptions.jvmTarget = "1.8"
        }
        testRuns["test"].executionTask.configure {
            useJUnit()
        }
    }
    js {
        nodejs{
            testTask {
                useMocha {
                    timeout = "30s"
                }
            }
        }
        browser{
            testTask {
                useKarma {
                    useChromeHeadless()
                }
            }
            binaries.executable()
        }
    }

    sourceSets {
        val commonMain by getting {
            dependencies {
                compileOnly("com.ditchoom:mqtt-4-models:1.0.17")
                compileOnly("com.ditchoom:mqtt-5-models:1.0.13")
                implementation("com.ditchoom:socket:1.0.31")
                implementation("com.ditchoom:buffer:1.0.54")
                implementation("com.ditchoom:mqtt-base-models:1.0.20")
                implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0")
                implementation("org.jetbrains.kotlinx:atomicfu:0.17.0")
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(kotlin("test"))
                implementation("com.ditchoom:mqtt-4-models:1.0.17")
                implementation("com.ditchoom:mqtt-5-models:1.0.13")
            }
        }

        val jvmMain by getting {
            kotlin.srcDir("src/commonJvmMain/kotlin")
        }
        val jvmTest by getting {
            kotlin.srcDir("src/commonJvmTest/kotlin")
        }

        val androidMain by getting {
            kotlin.srcDir("src/commonJvmMain/kotlin")
        }

        val androidTest by getting {
            kotlin.srcDir("src/commonJvmTest/kotlin")
        }

        val jsTest by getting {
            dependencies {
                implementation(kotlin("test-js"))
                implementation(npm("tcp-port-used", "1.0.1"))
            }
        }
    }
}

android {
    compileSdkVersion(31)
    sourceSets["main"].manifest.srcFile("src/androidMain/AndroidManifest.xml")
    defaultConfig {
        minSdkVersion(1)
        targetSdkVersion(31)
    }
    lintOptions {
        isQuiet = true
        isAbortOnError =  false
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
}

val javadocJar: TaskProvider<Jar> by tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
}

System.getenv("GITHUB_REPOSITORY")?.let {
    signing {

        useInMemoryPgpKeys("56F1A973", System.getenv("GPG_SECRET"), System.getenv("GPG_SIGNING_PASSWORD"))
        sign(publishing.publications)
    }


    val ossUser = System.getenv("SONATYPE_NEXUS_USERNAME")
    val ossPassword = System.getenv("SONATYPE_NEXUS_PASSWORD")

    val publishedGroupId: String by project
    val libraryName: String by project
    val libraryDescription: String by project
    val siteUrl: String by project
    val gitUrl: String by project
    val licenseName: String by project
    val licenseUrl: String by project
    val developerOrg: String by project
    val developerName: String by project
    val developerEmail: String by project
    val developerId: String by project

    val libraryVersion = if (System.getenv("GITHUB_RUN_NUMBER") != null) {
        "$libraryVersionPrefix${System.getenv("GITHUB_RUN_NUMBER")}"
    } else {
        "${libraryVersionPrefix}0-SNAPSHOT"
    }

    project.group = publishedGroupId
    project.version = libraryVersion

    publishing {
        publications.withType(MavenPublication::class) {
            groupId = publishedGroupId
            version = libraryVersion

            artifact(tasks["javadocJar"])

            pom {
                name.set(libraryName)
                description.set(libraryDescription)
                url.set(siteUrl)

                licenses {
                    license {
                        name.set(licenseName)
                        url.set(licenseUrl)
                    }
                }
                developers {
                    developer {
                        id.set(developerId)
                        name.set(developerName)
                        email.set(developerEmail)
                    }
                }
                organization {
                    name.set(developerOrg)
                }
                scm {
                    connection.set(gitUrl)
                    developerConnection.set(gitUrl)
                    url.set(siteUrl)
                }
            }
        }

        repositories {
            maven("https://oss.sonatype.org/service/local/staging/deploy/maven2/") {
                name = "sonatype"
                credentials {
                    username = ossUser
                    password = ossPassword
                }
            }
        }
    }

    nexusStaging {
        username = ossUser
        password = ossPassword
        packageGroup = publishedGroupId
    }
}