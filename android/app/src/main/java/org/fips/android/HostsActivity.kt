package org.fips.android

import android.os.Bundle
import android.text.format.DateUtils
import android.view.View
import android.view.inputmethod.EditorInfo
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import com.google.android.material.appbar.MaterialToolbar
import com.google.android.material.button.MaterialButton
import com.google.android.material.materialswitch.MaterialSwitch
import com.google.android.material.textfield.TextInputLayout

/**
 * The mesh-name address book: `home` → npub, so mesh apps can open
 * `home.fips`. See [HostsStore] for where the names live and why an edit
 * needs no reconnect — which is also why, unlike [RelaysActivity], there is
 * no "applied on leaving" hint and no rebind in onPause.
 *
 * Saving a name that already exists re-points it (tapping a row loads it into
 * the fields for exactly that); several names may share one npub.
 *
 * Below the user's own names: the optional pull from a node running fips-ui
 * ([HostsSync]) and the names it brought, read-only — they are replaced on
 * every sync, so editing them here would only be undone.
 */
class HostsActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_hosts)
        findViewById<MaterialToolbar>(R.id.toolbar).setNavigationOnClickListener { finish() }

        findViewById<MaterialButton>(R.id.host_add).setOnClickListener { add() }
        findViewById<EditText>(R.id.host_npub).setOnEditorActionListener { _, action, _ ->
            if (action == EditorInfo.IME_ACTION_DONE) add()
            action == EditorInfo.IME_ACTION_DONE
        }
        findViewById<MaterialButton>(R.id.sync_save).setOnClickListener { saveSync() }
        findViewById<MaterialButton>(R.id.sync_now).setOnClickListener {
            if (!HostsSync.syncNow(this)) {
                Ui.snack(it, "Connect first — names sync while fips2go is connected")
            }
        }
        findViewById<MaterialSwitch>(R.id.sync_enabled).setOnCheckedChangeListener { _, on ->
            findViewById<View>(R.id.sync_fields).visibility = if (on) View.VISIBLE else View.GONE
        }
        findViewById<MaterialButton>(R.id.synced_show_all).setOnClickListener {
            showAllSynced = true
            render()
        }
        loadSync()
        render()
    }

    override fun onResume() {
        super.onResume()
        // A sync may rewrite the file while this screen is open.
        HostsSync.listener = { render(); renderSyncStatus() }
        render()
        renderSyncStatus()
    }

    override fun onPause() {
        super.onPause()
        HostsSync.listener = null
    }

    /** Synced lists can be long; the first rows are enough until asked. */
    private var showAllSynced = false

    private fun loadSync() {
        val cfg = HostsSync.config(this)
        findViewById<MaterialSwitch>(R.id.sync_enabled).isChecked = cfg.enabled
        findViewById<View>(R.id.sync_fields).visibility = if (cfg.enabled) View.VISIBLE else View.GONE
        findViewById<EditText>(R.id.sync_from).setText(
            // Shown as the user's name for it when they have one.
            HostsStore.load(this).firstOrNull { it.npub == cfg.from }?.name ?: cfg.from
        )
        findViewById<EditText>(R.id.sync_port).setText(cfg.port.toString())
        findViewById<EditText>(R.id.sync_interval).setText(cfg.intervalMin.toString())
    }

    private fun saveSync() {
        val enabled = findViewById<MaterialSwitch>(R.id.sync_enabled).isChecked
        val fromLayout = findViewById<TextInputLayout>(R.id.sync_from_layout)
        val portLayout = findViewById<TextInputLayout>(R.id.sync_port_layout)
        val intervalLayout = findViewById<TextInputLayout>(R.id.sync_interval_layout)
        val typed = findViewById<EditText>(R.id.sync_from).text.toString()
        val port = findViewById<EditText>(R.id.sync_port).text.toString().trim().toIntOrNull()
        val interval = findViewById<EditText>(R.id.sync_interval).text.toString().trim().toIntOrNull()

        // A name the user already gave the node, or its npub.
        val byName = HostsStore.load(this).firstOrNull { it.name == HostsStore.normalizeName(typed) }
        val from = byName?.npub ?: HostsStore.normalizeNpub(typed)
        val own = runCatching { org.json.JSONObject(FipsNative.status()).optString("npub") }.getOrNull()
        fromLayout.error = when {
            !enabled -> null
            from == null -> "Enter the node's npub, or a name from your list"
            from == own -> "That is this device"
            else -> null
        }
        portLayout.error = if (port == null || port !in 1..65535) "1–65535" else null
        intervalLayout.error = if (interval == null || interval !in MIN_INTERVAL..MAX_INTERVAL) {
            "$MIN_INTERVAL–$MAX_INTERVAL"
        } else null
        if (enabled && (fromLayout.error != null || portLayout.error != null ||
                intervalLayout.error != null)
        ) return

        val previous = HostsSync.config(this)
        HostsSync.save(
            this,
            HostsSync.Config(
                enabled,
                from ?: previous.from,
                port?.takeIf { it in 1..65535 } ?: previous.port,
                interval?.takeIf { it in MIN_INTERVAL..MAX_INTERVAL } ?: previous.intervalMin,
            ),
        )
        Ui.snack(
            fromLayout,
            if (enabled && FipsVpnService.tunnelActive) "Saved — syncing"
            else if (enabled) "Saved — syncs once fips2go is connected" else if (HostsStore.loadSynced(this) != null) {
                "Sync off — synced names removed"
            } else "Sync off",
        )
    }

    private fun renderSyncStatus() {
        val cfg = HostsSync.config(this)
        val st = HostsSync.status(this)
        val text = findViewById<TextView>(R.id.sync_status)
        findViewById<MaterialButton>(R.id.sync_now).apply {
            isEnabled = cfg.enabled && !st.running
            visibility = if (cfg.enabled) View.VISIBLE else View.GONE
        }
        if (!cfg.enabled) {
            text.visibility = View.GONE
            return
        }
        text.visibility = View.VISIBLE
        // "just now" / "5 minutes ago" / "in 59 minutes", mid-sentence.
        fun ago(t: Long): String {
            val now = System.currentTimeMillis()
            if (kotlin.math.abs(now - t) < DateUtils.MINUTE_IN_MILLIS) return "just now"
            return DateUtils.getRelativeTimeSpanString(t, now, DateUtils.MINUTE_IN_MILLIS)
                .toString().replaceFirstChar { it.lowercase() }
        }
        text.text = buildString {
            when {
                st.running -> append("Syncing…")
                st.lastSuccess > 0 -> {
                    append("Last synced ${ago(st.lastSuccess)}: ${st.received} names")
                    if (st.skipped > 0) append(" (${st.skipped} invalid left out)")
                    append(".")
                }
                st.lastAttempt == 0L && st.error == null -> append("Not synced yet.")
            }
            st.error?.let {
                if (isNotEmpty()) append("\n")
                append(it)
            }
            if (st.chain.isNotEmpty() && st.lastSuccess > 0) {
                append("\nNames come from: ")
                append(chainText(st.chain, st.chainConfirmed))
            }
            if (!st.running && st.nextAttempt > System.currentTimeMillis() && FipsVpnService.tunnelActive) {
                append("\nNext sync ${ago(st.nextAttempt)}.")
            }
        }
    }

    /** `hub (master node) › relay › this device`, from the chain the upstream reported. */
    private fun chainText(chain: List<String>, confirmed: Boolean): String {
        val names = HostsStore.effective(this).associate { it.npub to it.name }
        val shown = chain.reversed().mapIndexed { i, npub ->
            val label = names[npub] ?: shortNpub(npub)
            when {
                i == 0 && confirmed -> "$label (master node)"
                i == 0 && chain.size > 1 -> "… › $label"
                else -> label
            }
        }
        return (shown + "this device").joinToString(" › ")
    }

    private fun add() {
        val nameField = findViewById<EditText>(R.id.host_name)
        val npubField = findViewById<EditText>(R.id.host_npub)
        val nameLayout = findViewById<TextInputLayout>(R.id.host_name_layout)
        val npubLayout = findViewById<TextInputLayout>(R.id.host_npub_layout)

        val name = HostsStore.normalizeName(nameField.text.toString())
        val npub = HostsStore.normalizeNpub(npubField.text.toString())
        val current = HostsStore.load(this)
        val replacing = current.any { it.name == name }
        nameLayout.error = HostsStore.nameError(name)
            ?: "At most ${HostsStore.MAX_HOSTS} names".takeIf {
                !replacing && current.size >= HostsStore.MAX_HOSTS
            }
        npubLayout.error = if (npub == null) "Not an npub — expected npub1…" else null
        if (nameLayout.error != null || npub == null) return

        val entry = HostsStore.Host(name, npub)
        val updated =
            if (replacing) current.map { if (it.name == name) entry else it } else current + entry
        if (!store(updated)) return
        nameField.setText("")
        npubField.setText("")
        nameField.requestFocus()
        if (replacing) Ui.snack(nameField, "$name.fips now points to ${shortNpub(npub)}")
        render()
    }

    /** False (with a message) when the file could not be written. */
    private fun store(hosts: List<HostsStore.Host>): Boolean {
        val failure = runCatching { HostsStore.save(this, hosts) }.exceptionOrNull() ?: return true
        Ui.snack(findViewById(R.id.host_list), "Could not save: ${failure.message}", long = true)
        return false
    }

    private fun render() {
        val hosts = HostsStore.load(this)
        val synced = HostsStore.loadSynced(this)?.hosts.orEmpty()
        val syncedByName = synced.associate { it.name to it.npub }
        findViewById<View>(R.id.host_empty).visibility =
            if (hosts.isEmpty()) View.VISIBLE else View.GONE
        findViewById<View>(R.id.host_list_card).visibility =
            if (hosts.isEmpty()) View.GONE else View.VISIBLE

        val list = findViewById<LinearLayout>(R.id.host_list)
        list.removeAllViews()
        for (host in hosts) {
            val fqdn = "${host.name}.fips"
            val row = layoutInflater.inflate(R.layout.item_host_edit, list, false)
            row.findViewById<TextView>(R.id.host_row_name).text =
                // The synced entry comes later in the file, so it is what resolves.
                if (syncedByName[host.name]?.let { it != host.npub } == true) {
                    "$fqdn · overridden by sync"
                } else fqdn
            row.findViewById<TextView>(R.id.host_row_npub).text = host.npub
            row.setOnClickListener {
                findViewById<EditText>(R.id.host_name).setText(host.name)
                findViewById<EditText>(R.id.host_npub).setText(host.npub)
            }
            val copy = row.findViewById<MaterialButton>(R.id.host_copy)
            copy.contentDescription = "Copy $fqdn"
            copy.setOnClickListener { Ui.copy(row, "fips name", fqdn, "$fqdn copied") }
            val remove = row.findViewById<MaterialButton>(R.id.host_remove)
            remove.contentDescription = "Remove $fqdn"
            remove.setOnClickListener {
                if (store(HostsStore.load(this).filter { it.name != host.name })) render()
            }
            list.addView(row)
        }

        findViewById<View>(R.id.synced_header).visibility =
            if (synced.isEmpty()) View.GONE else View.VISIBLE
        findViewById<View>(R.id.synced_list_card).visibility =
            if (synced.isEmpty()) View.GONE else View.VISIBLE
        val syncedList = findViewById<LinearLayout>(R.id.synced_list)
        syncedList.removeAllViews()
        val shown = if (showAllSynced) synced else synced.take(SYNCED_PREVIEW)
        for (host in shown) {
            val fqdn = "${host.name}.fips"
            val row = layoutInflater.inflate(R.layout.item_host_edit, syncedList, false)
            row.findViewById<TextView>(R.id.host_row_name).text = fqdn
            row.findViewById<TextView>(R.id.host_row_npub).text = host.npub
            row.isClickable = false
            row.background = null
            val copy = row.findViewById<MaterialButton>(R.id.host_copy)
            copy.contentDescription = "Copy $fqdn"
            copy.setOnClickListener { Ui.copy(row, "fips name", fqdn, "$fqdn copied") }
            row.findViewById<View>(R.id.host_remove).visibility = View.GONE
            syncedList.addView(row)
        }
        findViewById<MaterialButton>(R.id.synced_show_all).apply {
            visibility = if (shown.size < synced.size) View.VISIBLE else View.GONE
            text = "Show all ${synced.size}"
        }
    }

    companion object {
        private const val SYNCED_PREVIEW = 50
        /** Battery: nothing more often than every 15 minutes. */
        private const val MIN_INTERVAL = 15
        private const val MAX_INTERVAL = 1440
    }

    private fun shortNpub(npub: String): String =
        if (npub.length > 20) "${npub.take(10)}…${npub.takeLast(6)}" else npub
}
