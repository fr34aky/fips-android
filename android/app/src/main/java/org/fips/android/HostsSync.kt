package org.fips.android

import android.content.Context
import android.os.Handler
import android.os.Looper
import android.util.Log
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import org.json.JSONArray
import org.json.JSONObject

/**
 * Mesh names sync: pull the name list of another node running fips-ui into
 * the hosts file, the way fips-ui's own "Sync names from another node" does
 * (fips-ui `server/hosts-sync.ts`, `docs/hosts-sync.md`) — this phone is
 * always a follower, never serves a list itself.
 *
 * - Pull, while connected: every [Config.intervalMin] minutes (hourly by
 *   default — a phone pays for each wake-up) and on "Sync now", `GET http://[<upstream fips0 address>]:<port>/api/hosts`
 *   over the mesh. The request is made inside the shim
 *   ([FipsNative.meshHttpGet]) because this app's own sockets are outside its
 *   tunnel. Both ends are authenticated by the mesh: only the upstream's npub
 *   can answer from its address, and it admits this phone by its npub (a
 *   viewer on its "Web UI over the mesh" list).
 * - The names go into the marked block at the end of the hosts file
 *   ([HostsStore.saveSynced]), rewritten only when the list changed. The
 *   user's own entries stay; on a duplicate name the synced one wins.
 * - Every entry is checked like one typed in the editor; invalid ones are
 *   skipped and counted, at most [MAX_ENTRIES] are taken. An answer carrying
 *   an `error` (the upstream cannot read its own file) is never taken as "no
 *   names" — it would wipe the block.
 * - Upstream offline or refusing: the names synced last stay in effect and
 *   automatic attempts drop to once a day; "Sync now" tries at once. A
 *   connect does not pull by itself: it resumes the schedule, pulling only
 *   if a sync fell due while disconnected. Turning sync off removes the block.
 *
 * All runs, saves and removals happen one at a time on one worker thread.
 */
object HostsSync {
    private const val TAG = "fips-hosts-sync"

    /** At most this many names are taken from the upstream (fips-ui's cap). */
    const val MAX_ENTRIES = 2000

    /** Longest upstream chain accepted (deeper is treated like a loop, as in fips-ui). */
    private const val MAX_CHAIN = 16

    /** While the upstream is unreachable or refuses this node. */
    private const val OFFLINE_RETRY_MS = 24L * 60 * 60 * 1000

    /** After connect: give the node a moment to link up before the first pull. */
    private const val CONNECT_DELAY_MS = 10_000L

    /** The engine was mid-restart (a rebind): try again shortly, not an attempt. */
    private const val BUSY_RETRY_MS = 30_000L

    private const val TIMEOUT_MS = 20_000

    private val NPUB_RE = Regex("^npub1[02-9ac-hj-np-z]{58}$")

    private const val STATUS_PREFS = "hosts_sync"
    private const val STATUS_KEY = "status"

    data class Config(val enabled: Boolean, val from: String, val port: Int, val intervalMin: Int)

    data class Status(
        val running: Boolean = false,
        val lastAttempt: Long = 0,
        val lastSuccess: Long = 0,
        val lastChange: Long = 0,
        /** Names taken on the last successful sync, and invalid ones left out. */
        val received: Int = 0,
        val skipped: Int = 0,
        val error: String? = null,
        /** Since when the upstream has been unreachable (automatic syncs daily). */
        val unreachableSince: Long = 0,
        val nextAttempt: Long = 0,
        /** The upstream and the nodes it syncs from in turn, nearest first. */
        val chain: List<String> = emptyList(),
        /** Whether [chain] is known all the way up to the master node. */
        val chainConfirmed: Boolean = false,
    )

    private class SyncError(message: String, val offline: Boolean = false) : Exception(message)

    private val worker = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "fips-hosts-sync").apply { isDaemon = true }
    }
    private val main = Handler(Looper.getMainLooper())
    private var pending: ScheduledFuture<*>? = null

    /** Bumped by every save: a sync started under an older configuration does not write. */
    @Volatile private var generation = 0

    @Volatile private var current: Status? = null

    /** Called on the main thread after every status change (the open editor). */
    @Volatile var listener: (() -> Unit)? = null

    fun config(context: Context): Config {
        val p = ConfigStore.prefs(context)
        return Config(
            p.getBoolean(ConfigStore.HOSTS_SYNC, ConfigStore.DEF_HOSTS_SYNC),
            p.getString(ConfigStore.HOSTS_SYNC_FROM, "") ?: "",
            p.getInt(ConfigStore.HOSTS_SYNC_PORT, ConfigStore.DEF_HOSTS_SYNC_PORT),
            p.getInt(ConfigStore.HOSTS_SYNC_INTERVAL, ConfigStore.DEF_HOSTS_SYNC_INTERVAL_MIN),
        )
    }

    fun status(context: Context): Status = current ?: loadStatus(context).also { current = it }

    /**
     * Persist a new configuration (already validated by the editor), then
     * sync at once when connected — or remove the synced names when off.
     */
    fun save(context: Context, cfg: Config) {
        val app = context.applicationContext
        generation++
        ConfigStore.prefs(app).edit()
            .putBoolean(ConfigStore.HOSTS_SYNC, cfg.enabled)
            .putString(ConfigStore.HOSTS_SYNC_FROM, cfg.from)
            .putInt(ConfigStore.HOSTS_SYNC_PORT, cfg.port)
            .putInt(ConfigStore.HOSTS_SYNC_INTERVAL, cfg.intervalMin)
            .apply()
        setStatus(app, Status())
        schedule(app, 0)
    }

    /** "Sync now": always tries at once, whatever the backoff. */
    fun syncNow(context: Context) = schedule(context.applicationContext, 0)

    /**
     * The engine is up (FipsVpnService): resume the schedule. Unlike fips-ui
     * at start, a connect is not itself a reason to pull — phones reconnect
     * often — so only a sync that fell due meanwhile runs (after a moment for
     * the node to link up).
     */
    fun onConnected(context: Context) {
        val app = context.applicationContext
        if (!config(app).enabled) return
        val due = status(app).nextAttempt - System.currentTimeMillis()
        schedule(app, maxOf(CONNECT_DELAY_MS, due))
    }

    /** Disconnected: nothing can be fetched until the next connect. */
    @Synchronized
    fun onDisconnected() {
        pending?.cancel(false)
        pending = null
    }

    @Synchronized
    private fun schedule(app: Context, delayMs: Long) {
        pending?.cancel(false)
        pending = worker.schedule({ run(app) }, delayMs.coerceAtLeast(0), TimeUnit.MILLISECONDS)
    }

    /** Worker thread. Never throws; the outcome is in the status. */
    private fun run(app: Context) {
        val cfg = config(app)
        if (!cfg.enabled) {
            removeBlock(app)
            return
        }
        if (!FipsNative.isRunning()) {
            // A rebind restarts the engine for a couple of seconds; that is
            // not an attempt. Fully disconnected: the next connect retries.
            if (FipsVpnService.tunnelActive) schedule(app, BUSY_RETRY_MS)
            else setStatus(app, status(app).copy(error = "Not connected — names sync while fips2go is connected"))
            return
        }
        val gen = generation
        val started = System.currentTimeMillis()
        setStatus(app, status(app).copy(running = true, lastAttempt = started))
        var next: Status
        try {
            val (hosts, skipped, chain, confirmed) = fetch(app, cfg)
            if (gen != generation) return // reconfigured meanwhile: the new config decides
            val label = HostsStore.load(app).firstOrNull { it.npub == cfg.from }?.name
            val changed = HostsStore.saveSynced(app, cfg.from, label, hosts)
            val now = System.currentTimeMillis()
            next = status(app).copy(
                lastSuccess = now,
                lastChange = if (changed) now else status(app).lastChange,
                received = hosts.size, skipped = skipped, error = null, unreachableSince = 0,
                nextAttempt = now + cfg.intervalMin * 60_000L,
                chain = chain, chainConfirmed = confirmed,
            )
            Log.i(TAG, "synced ${hosts.size} names from ${cfg.from} (changed=$changed, skipped=$skipped)")
        } catch (e: Exception) {
            if (gen != generation) return
            if (!FipsNative.isRunning() && FipsVpnService.tunnelActive) {
                // The engine restarted under the request (a rebind).
                setStatus(app, status(app).copy(running = false))
                schedule(app, BUSY_RETRY_MS)
                return
            }
            val offline = e is SyncError && e.offline
            val now = System.currentTimeMillis()
            val prev = status(app)
            next = prev.copy(
                error = e.message ?: e.toString(),
                unreachableSince = if (offline) prev.unreachableSince.takeIf { it > 0 } ?: now else 0,
                nextAttempt = now + if (offline) OFFLINE_RETRY_MS else cfg.intervalMin * 60_000L,
                // Without a fresh answer only the configured upstream is certain.
                chain = listOf(cfg.from), chainConfirmed = false,
            )
            Log.i(TAG, "sync from ${cfg.from} failed: ${next.error}")
        }
        setStatus(app, next.copy(running = false))
        schedule(app, next.nextAttempt - System.currentTimeMillis())
    }

    private data class Fetched(
        val hosts: List<HostsStore.Host>, val skipped: Int, val chain: List<String>, val confirmed: Boolean,
    )

    private fun fetch(app: Context, cfg: Config): Fetched {
        val version = runCatching {
            app.packageManager.getPackageInfo(app.packageName, 0).versionName
        }.getOrNull() ?: ""
        // The upstream lists this node among the nodes syncing from it, with
        // version and interval (fips-ui's x-fips-ui-sync header).
        val headers = JSONObject()
            .put("x-fips-ui-sync", "version=fips2go-$version;interval=${cfg.intervalMin}")
            .put("user-agent", "fips2go-sync")
        val res = JSONObject(
            FipsNative.meshHttpGet(cfg.from, cfg.port, "/api/hosts", headers.toString(), TIMEOUT_MS)
        )
        if (res.has("error")) {
            val unreachable = res.optBoolean("unreachable")
            throw SyncError(
                // fips-ui's guard drops TCP from npubs not on its list in the
                // kernel, so a missing viewer entry looks exactly like this.
                if (unreachable) "${res.getString("error")}. On that node, is fips-ui's \"Web UI " +
                    "over the mesh\" on port ${cfg.port}, with this device's npub as a viewer? " +
                    "Retrying once a day, or use Sync now"
                else res.getString("error"),
                offline = unreachable,
            )
        }
        val code = res.getInt("status")
        val body = runCatching { JSONObject(res.getString("body")) }.getOrNull()
        val bodyError = body?.optString("error")?.takeIf { it.isNotEmpty() }
        if (code == 403) {
            val own = runCatching { JSONObject(FipsNative.status()).optString("npub") }.getOrNull()
                ?.takeIf { it.isNotEmpty() } ?: "this device's npub"
            throw SyncError(
                "The upstream node refused this device${bodyError?.let { " ($it)" } ?: ""}: on it, " +
                    "add $own as a viewer under Access → Web UI over the mesh, then use Sync now",
                offline = true,
            )
        }
        val entries = body?.optJSONArray("entries")
        if (code != 200 || entries == null) {
            throw SyncError("The upstream node answered $code${bodyError?.let { ": $it" } ?: ""}")
        }
        if (bodyError != null) {
            throw SyncError("The upstream node cannot read its hosts file ($bodyError); keeping the names synced last")
        }

        // Only well-formed entries are taken; the last one wins on a duplicate name.
        val byName = LinkedHashMap<String, String>()
        var skipped = (entries.length() - MAX_ENTRIES).coerceAtLeast(0)
        for (i in 0 until minOf(entries.length(), MAX_ENTRIES)) {
            val e = entries.optJSONObject(i)
            val name = e?.optString("hostname")?.lowercase() ?: ""
            val npub = e?.optString("npub") ?: ""
            if (HostsStore.nameError(name) != null || !NPUB_RE.matches(npub)) {
                skipped++
                continue
            }
            byName.remove(name)
            byName[name] = npub
        }

        // The upstream's chain (itself first): this node in it would mean a
        // loop. A phone never serves names, so this is a guard, not a feature.
        val rawChain = body.optJSONArray("chain")
        val chain = rawChain?.let { a ->
            (0 until a.length()).map { a.optString(it) }.filter { NPUB_RE.matches(it) }
        } ?: listOf(cfg.from)
        val own = runCatching { JSONObject(FipsNative.status()).optString("npub") }.getOrNull()
        if (!own.isNullOrEmpty() && own in chain) {
            throw SyncError("Sync loop: the upstream node syncs its names from this device; keeping the current names")
        }
        if (chain.size > MAX_CHAIN) {
            throw SyncError("The chain of upstream nodes is longer than $MAX_CHAIN nodes; keeping the current names")
        }
        return Fetched(
            byName.map { (name, npub) -> HostsStore.Host(name, npub) },
            skipped, chain, rawChain != null && body.optBoolean("chainComplete"),
        )
    }

    private fun removeBlock(app: Context) {
        val synced = HostsStore.loadSynced(app) ?: return
        try {
            HostsStore.saveSynced(app, synced.master, null, null)
            setStatus(app, Status(lastChange = System.currentTimeMillis()))
        } catch (e: Exception) {
            setStatus(app, status(app).copy(error = "Could not remove the synced names: ${e.message}"))
        }
    }

    private fun setStatus(app: Context, s: Status) {
        current = s
        val json = JSONObject()
            .put("lastAttempt", s.lastAttempt).put("lastSuccess", s.lastSuccess)
            .put("lastChange", s.lastChange).put("received", s.received).put("skipped", s.skipped)
            .put("unreachableSince", s.unreachableSince).put("nextAttempt", s.nextAttempt)
            .put("chain", JSONArray(s.chain)).put("chainConfirmed", s.chainConfirmed)
        s.error?.let { json.put("error", it) }
        app.getSharedPreferences(STATUS_PREFS, Context.MODE_PRIVATE).edit()
            .putString(STATUS_KEY, json.toString()).apply()
        main.post { listener?.invoke() }
    }

    private fun loadStatus(context: Context): Status {
        val raw = context.getSharedPreferences(STATUS_PREFS, Context.MODE_PRIVATE)
            .getString(STATUS_KEY, null) ?: return Status()
        val j = runCatching { JSONObject(raw) }.getOrNull() ?: return Status()
        val chain = j.optJSONArray("chain")
        return Status(
            lastAttempt = j.optLong("lastAttempt"), lastSuccess = j.optLong("lastSuccess"),
            lastChange = j.optLong("lastChange"), received = j.optInt("received"),
            skipped = j.optInt("skipped"), error = j.optString("error").takeIf { it.isNotEmpty() },
            unreachableSince = j.optLong("unreachableSince"), nextAttempt = j.optLong("nextAttempt"),
            chain = chain?.let { a -> (0 until a.length()).map { a.optString(it) } } ?: emptyList(),
            chainConfirmed = j.optBoolean("chainConfirmed"),
        )
    }
}
