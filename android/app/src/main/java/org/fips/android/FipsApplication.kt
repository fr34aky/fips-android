package org.fips.android

import android.app.Application

/**
 * Process-wide setup that must happen before any activity is created: the
 * colour scheme (dark by default, Settings → Appearance), which
 * `AppCompatDelegate` applies to every activity from then on.
 */
class FipsApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        ConfigStore.applyTheme(this)
    }
}
