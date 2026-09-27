package org.fips.android

import android.content.Context
import java.io.File
import org.json.JSONObject

/**
 * The user's mesh names: readable name → npub, so a mesh app can use
 * `home.fips` instead of `npub1….fips`.
 *
 * The file IS the store — there is no copy in the prefs. It is a fips hosts
 * file (`name npub` per line, `#` comments; the format of `/etc/fips/hosts`)
 * in the app's private files dir, and the shim's DNS proxy reads the very same
 * file ([ConfigStore.buildConfigJson] passes its path as `hosts_path`),
 * re-reading it whenever the mtime changes. So an edit applies on the next
 * lookup: no rebind, no node restart, unlike relays or mesh apps.
 *
 * Writes go through a temp file + rename because the shim may read at any
 * moment, and a half-written file would parse as a shorter list.
 *
 * Names pulled from another node ([HostsSync]) live in one marked block at
 * the END of the same file, in fips-ui's format:
 *
 *     # >>> fips-ui sync from npub1… (hub): managed by fips2go, …
 *     nas npub1…
 *     # <<< fips-ui sync
 *
 * The user's own entries are everything outside it. Coming last, a synced
 * name wins over a local one of the same name (fips uses the last line).
 * Reads and writes are serialised on this object: the sync thread rewrites
 * the block while the editor may be saving the local lines.
 */
object HostsStore {

    data class Host(val name: String, val npub: String)

    /** The block synced from [master]. */
    data class Synced(val master: String, val hosts: List<Host>)

    /** A hosts file is typed by hand; keep it phone-sized. */
    const val MAX_HOSTS = 64

    private const val FILE = "hosts"

    private val SYNC_BEGIN = Regex("^# >>> fips-ui sync from (npub1[02-9ac-hj-np-z]{58})\\b")
    private val SYNC_END = Regex("^# <<< fips-ui sync\\b")

    fun file(context: Context) = File(context.filesDir, FILE)

    /** The file split into the user's lines and the synced block (with its master). */
    private class Split(val local: List<String>, val block: List<String>?, val master: String?)

    private fun split(text: String): Split {
        val lines = text.lines().let { if (it.lastOrNull() == "") it.dropLast(1) else it }
        val start = lines.indexOfFirst { SYNC_BEGIN.containsMatchIn(it.trim()) }
        if (start < 0) return Split(lines, null, null)
        var end = (start + 1 until lines.size).firstOrNull { SYNC_END.containsMatchIn(lines[it].trim()) }
            ?: (lines.size - 1)
        // The blank line written before the block belongs to it.
        val before = if (start > 0 && lines[start - 1].isBlank()) start - 1 else start
        return Split(
            lines.subList(0, before) + lines.subList(end + 1, lines.size),
            lines.subList(start, end + 1),
            SYNC_BEGIN.find(lines[start].trim())!!.groupValues[1],
        )
    }

    private fun read(context: Context): String? = runCatching { file(context).readText() }.getOrNull()

    /** Entries in file order; lines fips would skip are skipped here too. */
    private fun parse(lines: List<String>): List<Host> {
        val hosts = LinkedHashMap<String, Host>()
        for (line in lines) {
            val trimmed = line.trim()
            if (trimmed.isEmpty() || trimmed.startsWith("#")) continue
            val fields = trimmed.split(Regex("\\s+"))
            if (fields.size != 2 || nameError(fields[0]) != null) continue
            // A repeated name: the later line wins, as in fips's HostMap.
            val name = fields[0].lowercase()
            hosts.remove(name)
            hosts[name] = Host(name, fields[1])
        }
        return hosts.values.toList()
    }

    /** The user's own entries (outside the synced block): what the editor changes. */
    @Synchronized
    fun load(context: Context): List<Host> {
        val text = read(context) ?: return emptyList()
        return parse(split(text).local)
    }

    /**
     * What resolves: the user's entries and the synced ones, the synced entry
     * winning on a duplicate name (it comes later in the file). For lookups
     * and display elsewhere — the editor works on [load] and [loadSynced].
     */
    @Synchronized
    fun effective(context: Context): List<Host> {
        val text = read(context) ?: return emptyList()
        val s = split(text)
        return parse(s.local + (s.block ?: emptyList()))
    }

    /** The block synced from another node, if any. */
    @Synchronized
    fun loadSynced(context: Context): Synced? {
        val text = read(context) ?: return null
        val s = split(text)
        return Synced(s.master ?: return null, parse(s.block ?: return null))
    }

    /** Replace the user's own entries; the synced block is kept as it is, at the end. */
    @Synchronized
    fun save(context: Context, hosts: List<Host>) {
        val block = read(context)?.let { split(it).block }
        val out = hosts.map { "${it.name} ${it.npub}" }.toMutableList()
        if (block != null) out += listOf("") + block
        write(context, out)
    }

    /**
     * Replace the synced block with [hosts] from [master] ([label]: a name for
     * it in the header), or remove it when [hosts] is null. The user's lines
     * are untouched. Returns whether the file changed.
     */
    @Synchronized
    fun saveSynced(context: Context, master: String, label: String?, hosts: List<Host>?): Boolean {
        val text = read(context)
        val local = (text?.let { split(it).local } ?: emptyList()).toMutableList()
        while (local.isNotEmpty() && local.last().isBlank()) local.removeAt(local.size - 1)
        if (hosts != null) {
            local += ""
            local += "# >>> fips-ui sync from $master${label?.let { " ($it)" } ?: ""}: " +
                "managed by fips2go, edits here are replaced on the next sync"
            local += hosts.map { "${it.name} ${it.npub}" }
            local += "# <<< fips-ui sync"
        }
        val content = if (local.isEmpty()) "" else local.joinToString("\n", postfix = "\n")
        if (content == (text ?: "")) return false
        writeText(context, content)
        return true
    }

    private fun write(context: Context, lines: List<String>) =
        writeText(context, lines.joinToString("") { "$it\n" })

    private fun writeText(context: Context, content: String) {
        val target = file(context)
        val tmp = File(target.parentFile, "$FILE.tmp")
        tmp.writeText(content)
        if (!tmp.renameTo(target)) {
            tmp.delete()
            throw java.io.IOException("could not replace ${target.name}")
        }
    }

    /** What the user typed, minus a pasted `.fips` and any capitals. */
    fun normalizeName(typed: String) = typed.trim().lowercase().removeSuffix(".fips")

    /**
     * Why [name] cannot be a mesh name, or null when it can. Mirrors fips's
     * `validate_hostname` (`src/upper/hosts.rs`) — the shim drops a line that
     * fails it, so anything accepted here that fips rejects would be listed
     * in the app and never resolve.
     */
    fun nameError(name: String): String? = when {
        name.isEmpty() -> "Enter a name"
        name.length > 63 -> "At most 63 characters"
        name.lowercase().startsWith("npub1") -> "A name cannot start with npub1"
        name.startsWith("-") || name.endsWith("-") -> "Cannot start or end with a hyphen"
        name.any { !(it in 'a'..'z' || it in 'A'..'Z' || it in '0'..'9' || it == '-') } ->
            "Letters, digits and hyphens only — no dots or spaces"
        else -> null
    }

    /**
     * Canonical npub for what the user typed (with or without a pasted
     * `.fips`), or null if it is not one. Validated by the same code that
     * will resolve it.
     */
    fun normalizeNpub(typed: String): String? {
        val raw = typed.trim().lowercase().removeSuffix(".fips")
        if (raw.isEmpty()) return null
        val info = runCatching { JSONObject(FipsNative.resolveNpub(raw)) }.getOrNull()
        return info?.optString("npub")?.takeIf { it.startsWith("npub1") }
    }
}
