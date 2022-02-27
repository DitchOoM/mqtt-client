package com.ditchoom.mqtt.client

import android.content.Context
import android.os.Parcelable

interface ParcelablePersistenceFactory : Parcelable {
    fun newPersistence(context: Context): Persistence

    companion object {
        const val BUNDLE_KEY = "ParcelablePersistenceFactory_BundleKey"
    }
}