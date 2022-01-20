@file:OptIn(ExperimentalMultiplatform::class)

package com.ditchoom.mqtt.client

@OptionalExpectation
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.BINARY)
expect annotation class Parcelize()