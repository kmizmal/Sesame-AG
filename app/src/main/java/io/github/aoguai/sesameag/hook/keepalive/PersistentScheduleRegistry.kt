package io.github.aoguai.sesameag.hook.keepalive

import android.content.Context
import com.fasterxml.jackson.core.type.TypeReference
import io.github.aoguai.sesameag.hook.AccountSessionCoordinator
import io.github.aoguai.sesameag.hook.AccountSlotRegistry
import io.github.aoguai.sesameag.hook.ApplicationHookCore
import io.github.aoguai.sesameag.util.DataStore
import io.github.aoguai.sesameag.util.Files
import io.github.aoguai.sesameag.util.Log
import io.github.aoguai.sesameag.util.TimeUtil
import io.github.aoguai.sesameag.storage.StoreAccess
import org.json.JSONArray
import org.json.JSONObject
import java.io.RandomAccessFile
import java.nio.channels.FileLock
import java.nio.channels.FileLockInterruptionException

/** 持久任务调度：保存显式注册的待办及执行状态；模块返回 DEFERRED 不会自动注册待办。 */
object PersistentScheduleRegistry {
    private const val TAG = "PersistentScheduleRegistry"
    private const val STORE_KEY = "persistentSchedules"
    private const val RETAIN_FINISHED_MS = 24 * 60 * 60 * 1000L
    private const val MAX_DEFERRED_ATTEMPTS = 3

    /** 全局轮询/唤醒的投递重试上限远高于业务子任务：它们只受补执行宽限窗约束。 */
    private const val MAX_GLOBAL_DEFERRED_ATTEMPTS = 40

    /** 全局轮询/唤醒在多次投递失败后的退避间隔，避免在 Doze 里高频空转。 */
    private const val GLOBAL_DEFER_BACKOFF_MS = 5 * 60 * 1000L

    /** QUEUED 表示「已被认领、即将执行」；长时间停留说明入口提前返回，不能一直封死主流程派发。 */
    private const val STALE_QUEUED_CHILD_MS = 5 * 60 * 1000L

    /** 宿主进程重启后重新排期的延迟，避免与启动期初始化抢资源。 */
    private const val RECOVERY_RETRY_DELAY_MS = 30 * 1000L

    /**
     * 全局轮询/唤醒停在 QUEUED 超过该时长，说明「已认领但没有派发」：
     * 主任务长时间不结束、或派发入口提前返回时，记录既不会被路由（只处理 SCHEDULED），
     * 也不会有任何闹钟（排闹钟同样只取 SCHEDULED），于是一次唤醒就被静默吞掉。
     */
    private const val STALE_GLOBAL_QUEUE_MS = 30 * 60 * 1000L

    /**
     * 全局轮询/唤醒停在 RUNNING 超过该时长，说明「已认领但主任务没走完」：
     * 主任务被静默取消、或在派发前就断链时，记录既不会被路由，也不会有闹钟，
     * 并且还会一直挡着 canScheduleDispatch（RUNNING 在 activeModuleChildStates 里），
     * 于是后续所有触发都被排队吞掉。业务子任务仍按各自超时处理，不受影响。
     */
    private const val STALE_GLOBAL_RUNNING_MS = 60 * 60 * 1000L

    /** 重排扫描本身有磁盘开销，限制最小间隔。 */
    private const val STALE_QUEUE_SWEEP_INTERVAL_MS = 5 * 60 * 1000L
    private val activeModuleChildStates = setOf(
        PersistentScheduleState.QUEUED,
        PersistentScheduleState.RUNNING,
    )
    private val terminalScheduleStates = setOf(
        PersistentScheduleState.FIRED,
        PersistentScheduleState.FAILED,
        PersistentScheduleState.EXPIRED,
    )

    private val scheduleListType = object : TypeReference<MutableList<PersistentSchedule>>() {}

    @Volatile
    private var storageReady = false

    // 普通查询复用完整快照；认领和更新在存储事务内从磁盘刷新。
    @Volatile
    private var cache: List<PersistentSchedule>? = null
    private val registryLockDepth = ThreadLocal<Int>()
    private var recoveredExecutionsInProcess = false

    @Volatile
    private var lastStaleQueueSweepAtMs = 0L

    data class ReconcileResult(
        val dueSchedules: List<PersistentSchedule>,
        val rescheduledCount: Int,
        val expiredCount: Int,
    )

    fun upsert(
        context: Context,
        schedule: PersistentSchedule,
    ): PersistentSchedule = try {
        var removed = emptyList<PersistentSchedule>()
        var registered = false
        val effectiveSchedule = withRegistryLock {
            if (!AccountSlotRegistry.isExecutableUser(schedule.ownerUserId)) {
                return@withRegistryLock schedule.withFailure("account_slot_inactive")
            }
            val now = System.currentTimeMillis()
            val normalized = schedule.copy(
                updatedAtMs = now,
                state = PersistentScheduleState.SCHEDULED,
                lastError = null,
            )
            val prepared = PersistentLaunchPolicy.prepareScheduleForRegistration(context, normalized)
            val effective = prepared.schedule
            val schedules = loadMutable()
            val globalSuccessor = effective.kind in setOf(
                PersistentScheduleKind.GLOBAL_POLL,
                PersistentScheduleKind.GLOBAL_WAKEUP,
                PersistentScheduleKind.GLOBAL_PREWAKEUP,
            )
            val replaced = schedules.filter {
                val currentOccurrence = it.state == PersistentScheduleState.QUEUED ||
                    it.state == PersistentScheduleState.RUNNING ||
                    (it.state == PersistentScheduleState.SCHEDULED && it.lastFireAtMs > 0L)
                it.id == effective.id ||
                    (effective.dedupeKey.isNotBlank() && it.dedupeKey == effective.dedupeKey &&
                        !(globalSuccessor && currentOccurrence &&
                            it.ownerUserId == effective.ownerUserId && it.sessionEpoch == effective.sessionEpoch))
            }
            if (prepared.blockedReason != null) {
                removed = replaced
                if (removed.isNotEmpty()) {
                    schedules.removeAll(removed.toSet())
                    save(schedules)
                }
                Log.record(TAG, "持久调度已因禁止系统调度前台拉起目标应用而未注册[${effective.name}]")
                return@withRegistryLock effective.withFailure(prepared.blockedReason, now)
            }
            // 同一份计划重排（例如每次唤醒都重算 0 点/下次唤醒）时物理闹钟注册仍可能失败；
            // 那种情况下这里必须真的重写一次记录并重排闹钟，不能因为「内容没变」就直接返回旧记录——
            // 旧记录的状态可能与新计划不一致，返回它之后调用方会把新计划当成已注册而不再重试。
            replaced.firstOrNull { isSameScheduledTask(it, effective) }?.let { existing ->
                if (existing.state == PersistentScheduleState.SCHEDULED &&
                    existing.lastError == null &&
                    !SystemWakeScheduler.schedule(context, existing, silent = true)
                ) {
                    Log.record(TAG, "持久调度重排物理闹钟失败[${existing.name}]，保留记录待下次重排")
                }
                return@withRegistryLock existing
            }
            removed = replaced
            schedules.removeAll(removed.toSet())
            schedules.add(effective)
            save(schedules)
            registered = true
            effective
        }
        if (!registered) {
            if (removed.isNotEmpty()) {
                removed.forEach { StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(it.id) } }
                SystemWakeScheduler.schedule(context, effectiveSchedule, silent = true)
            }
            effectiveSchedule
        } else if (SystemWakeScheduler.schedule(context, effectiveSchedule, silent = true)) {
            removed.forEach { StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(it.id) } }
            Log.runtime(TAG, "${if (removed.isEmpty()) "新增" else "替换"}持久调度[${effectiveSchedule.name}] 已重排物理闹钟")
            effectiveSchedule
        } else {
            withRegistryLock {
                val current = loadMutable()
                // 不覆盖系统重排期间其他事务新提交的调度。
                if (current.firstOrNull { it.id == effectiveSchedule.id } == effectiveSchedule) {
                    current.removeAll { it.id == effectiveSchedule.id }
                    removed.forEach { previous ->
                        if (current.none { it.id == previous.id ||
                                (previous.dedupeKey.isNotBlank() && it.dedupeKey == previous.dedupeKey && it !in removed) }) {
                            current.add(previous)
                        }
                    }
                    save(current)
                }
            }
            SystemWakeScheduler.schedule(context, effectiveSchedule, silent = true)
            Log.record(TAG, "持久调度注册失败，已恢复旧调度[${effectiveSchedule.name}]")
            effectiveSchedule.withFailure("system_alarm_schedule_failed")
        }
    } catch (e: InterruptedException) {
        throw e
    } catch (e: Exception) {
        Log.printStackTrace(TAG, "持久调度读写失败[${schedule.name}]", e)
        schedule.withFailure("persistent_storage_unavailable")
    }

    /** 业务续接更新进度，不改变本次调度状态或重新注册闹钟。 */
    fun updatePayload(id: String, ownerUserId: String, sessionEpoch: Long, payloadJson: String): Boolean {
        var updated = false
        updateSchedule(id) { current ->
            if (current.ownerUserId == ownerUserId && current.sessionEpoch == sessionEpoch &&
                current.kind == PersistentScheduleKind.MODULE_CHILD
            ) {
                updated = true
                current.copy(payloadJson = payloadJson, updatedAtMs = System.currentTimeMillis())
            } else {
                current
            }
        }
        return updated
    }

    fun removeByDedupeKey(
        context: Context?,
        dedupeKey: String,
    ): Int {
        if (dedupeKey.isBlank()) return 0
        val removed = withRegistryLock {
            val schedules = loadMutable()
            val removed = schedules.filter { it.dedupeKey == dedupeKey }
            if (removed.isNotEmpty()) {
                schedules.removeAll(removed.toSet())
                save(schedules)
            }
            removed
        }
        if (removed.isEmpty()) return 0
        removed.forEach { StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(it.id) } }
        context?.let { ctx -> SystemWakeScheduler.schedule(ctx, removed.first(), silent = true) }
        if (removed.any { it.kind == PersistentScheduleKind.MODULE_CHILD }) {
            ApplicationHookCore.dispatchIfNeeded()
        }
        return removed.size
    }

    fun removeByName(
        context: Context?,
        name: String,
    ): Int {
        if (name.isBlank()) return 0
        val removed = withRegistryLock {
            val schedules = loadMutable()
            val removed = schedules.filter { it.name == name }
            if (removed.isNotEmpty()) {
                schedules.removeAll(removed.toSet())
                save(schedules)
            }
            removed
        }
        if (removed.isEmpty()) return 0
        removed.forEach { StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(it.id) } }
        context?.let { ctx -> SystemWakeScheduler.schedule(ctx, removed.first(), silent = true) }
        return removed.size
    }

    fun removeByNameExceptDedupeKey(
        context: Context?,
        name: String,
        keepDedupeKey: String,
        silent: Boolean = false,
    ): Int {
        if (name.isBlank() || keepDedupeKey.isBlank()) return 0
        val removed = withRegistryLock {
            val schedules = loadMutable()
            val removed = schedules.filter { it.name == name && it.dedupeKey != keepDedupeKey }
            if (removed.isNotEmpty()) {
                schedules.removeAll(removed.toSet())
                save(schedules)
            }
            removed
        }
        if (removed.isEmpty()) return 0
        removed.forEach { StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(it.id) } }
        context?.let { ctx -> SystemWakeScheduler.schedule(ctx, removed.first(), silent = silent) }
        return removed.size
    }

    fun cancelByOwner(
        context: Context?,
        ownerUserId: String,
    ): Int {
        val safeOwnerUserId = ownerUserId.trim()
        if (safeOwnerUserId.isEmpty()) return 0
        val removed = withRegistryLock {
            val schedules = loadMutable()
            val removed = schedules.filter { it.ownerUserId?.trim() == safeOwnerUserId }
            if (removed.isNotEmpty()) {
                schedules.removeAll(removed.toSet())
                save(schedules)
            }
            removed
        }
        if (removed.isEmpty()) return 0
        removed.forEach { StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(it.id) } }
        context?.let { ctx -> SystemWakeScheduler.schedule(ctx, PersistentSchedule(), silent = true) }
        if (removed.any { it.kind == PersistentScheduleKind.MODULE_CHILD }) {
            ApplicationHookCore.dispatchIfNeeded()
        }
        return removed.size
    }

    fun get(id: String): PersistentSchedule? {
        if (id.isBlank()) return null
        if (!ensureStorage()) return null
        return loadMutable().firstOrNull { it.id == id }
    }

    fun listFresh(): List<PersistentSchedule> = withRegistryLock { list() }

    fun list(): List<PersistentSchedule> {
        if (!ensureStorage()) return emptyList()
        return loadMutable().toList()
    }

    /**
     * 把长期停在 QUEUED 的全局轮询/唤醒重新排入 SCHEDULED。
     *
     * 只处理 GLOBAL_* 的 QUEUED：RUNNING 表示主任务确实在执行，MODULE_CHILD 由业务自行续接。
     */
    fun requeueStaleGlobalSchedules(now: Long = System.currentTimeMillis()): Int {
        if (now - lastStaleQueueSweepAtMs < STALE_QUEUE_SWEEP_INTERVAL_MS) return 0
        lastStaleQueueSweepAtMs = now
        return try {
            withRegistryLock {
                if (!ensureStorage()) return@withRegistryLock 0
                val schedules = loadMutable()
                var requeued = 0
                for (index in schedules.indices) {
                    val schedule = schedules[index]
                    if (!schedule.isGlobalKind() || schedule.state !in setOf(
                            PersistentScheduleState.QUEUED,
                            PersistentScheduleState.RUNNING,
                        )
                    ) {
                        continue
                    }
                    val staleAfterMs = if (schedule.state == PersistentScheduleState.QUEUED) {
                        STALE_GLOBAL_QUEUE_MS
                    } else {
                        STALE_GLOBAL_RUNNING_MS
                    }
                    if (now - schedule.updatedAtMs <= staleAfterMs) continue
                    Log.record(
                        TAG,
                        "全局调度长期停在${schedule.state}，重新排队[${schedule.name}] id=${schedule.id} " +
                            "停留=${(now - schedule.updatedAtMs) / 1000}s",
                    )
                    schedules[index] = schedule.copy(
                        state = PersistentScheduleState.SCHEDULED,
                        triggerAtMs = now + RECOVERY_RETRY_DELAY_MS,
                        updatedAtMs = now,
                        lastError = if (schedule.state == PersistentScheduleState.QUEUED) {
                            "stale_queue_requeued"
                        } else {
                            "stale_running_requeued"
                        },
                    )
                    requeued++
                }
                if (requeued > 0) save(schedules)
                requeued
            }
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            Log.printStackTrace(TAG, "全局调度排队超时回收失败", e)
            0
        }
    }

    /**
     * Checks only the current session identity and persisted execution state.  Task names and
     * payload text are intentionally not part of the admission decision.
     */
    fun hasActiveModuleChild(
        ownerUserId: String?,
        sessionEpoch: Long,
        now: Long = System.currentTimeMillis(),
    ): Boolean {
        val safeOwnerUserId = ownerUserId?.trim().orEmpty()
        if (safeOwnerUserId.isEmpty() || sessionEpoch <= 0L || !ensureStorage()) return false
        return loadMutable().any { schedule ->
                schedule.kind == PersistentScheduleKind.MODULE_CHILD &&
                schedule.ownerUserId?.trim() == safeOwnerUserId &&
                schedule.sessionEpoch == sessionEpoch &&
                schedule.state in activeModuleChildStates &&
                // 卡死的 QUEUED 记录不能让主流程派发永久停摆；RUNNING 仍按业务耗时判定。
                !(schedule.state == PersistentScheduleState.QUEUED &&
                    now - schedule.updatedAtMs > STALE_QUEUED_CHILD_MS)
        }
    }

    /**
     * 只判断当前会话是否还有一条可执行的全局轮询/唤醒。
     *
     * 全局记录的收敛点全在这里，看门狗据此决定是否需要补登记下一轮。
     */
    fun hasRunnableGlobalSchedule(
        ownerUserId: String?,
        sessionEpoch: Long,
    ): Boolean {
        val safeOwnerUserId = ownerUserId?.trim().orEmpty()
        if (safeOwnerUserId.isEmpty() || sessionEpoch <= 0L || !ensureStorage()) return false
        return loadMutable().any { schedule ->
            schedule.isGlobalKind() &&
                schedule.state == PersistentScheduleState.SCHEDULED &&
                schedule.ownerUserId?.trim() == safeOwnerUserId &&
                schedule.sessionEpoch == sessionEpoch
        }
    }

    /**
     * One physical Alarm may represent several due schedules.  Each schedule still passes through
     * its existing Router state machine, preserving per-id queueing, session validation and retry.
     */
    fun fireDueSchedules(
        context: Context,
        source: String,
        now: Long = System.currentTimeMillis(),
    ): Int {
        val due =
            listFresh()
                .asSequence()
                .filter { schedule ->
                    schedule.state == PersistentScheduleState.SCHEDULED &&
                        schedule.triggerAtMs <= now
                }.sortedWith(
                    compareBy<PersistentSchedule> {
                        when (it.effectivePrecisionPolicy()) {
                            PersistentSchedulePrecisionPolicy.HARD_DEADLINE_CHILD -> 0
                            PersistentSchedulePrecisionPolicy.USER_EXACT -> 1
                            else -> 2
                        }
                    }.thenBy { it.triggerAtMs },
                ).toList()
        try {
            due.forEach { schedule -> ScheduledTaskRouter.fire(context, schedule, source) }
            return due.size
        } finally {
            SystemWakeScheduler.schedule(context, PersistentSchedule(), silent = true)
        }
    }

    fun clearAll(context: Context?) {
        val schedules = withRegistryLock {
            val schedules = loadMutable()
            if (schedules.isNotEmpty()) save(emptyList())
            schedules
        }
        if (schedules.isEmpty()) return
        schedules.forEach { StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(it.id) } }
        context?.let { ctx -> SystemWakeScheduler.schedule(ctx, PersistentSchedule(), silent = true) }
    }

    /** 在宿主发布首个运行时会话前调用；恢复广播不能重置当前进程的 Worker。 */
    internal fun recoverInterruptedExecutions(ownerUserId: String, sessionEpoch: Long) = withRegistryLock {
        if (recoveredExecutionsInProcess || ownerUserId.isBlank() || sessionEpoch <= 0L || !ensureStorage()) {
            return@withRegistryLock
        }
        val now = System.currentTimeMillis()
        val schedules = loadMutable()
        val recovered = schedules.map { schedule ->
            if (schedule.ownerUserId?.trim() == ownerUserId.trim() &&
                schedule.sessionEpoch == sessionEpoch && schedule.state in activeModuleChildStates
            ) {
                StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(schedule.id) }
                if (schedule.isGlobalKind()) {
                    // 全局轮询/唤醒不是业务子任务：进程退出只代表这次投递被打断，
                    // 判成 FAILED 会让整条调度链再没有任何闹钟（表现为定时任务永久休眠）。
                    Log.error(
                        TAG,
                        "持久调度因宿主进程重启而重新排期[${schedule.name}] id=${schedule.id} state=${schedule.state}",
                    )
                    schedule.copy(
                        state = PersistentScheduleState.SCHEDULED,
                        triggerAtMs = maxOf(schedule.firstDueAtMs(), now + RECOVERY_RETRY_DELAY_MS),
                        updatedAtMs = now,
                        lastError = "host_process_restarted_requeued",
                    )
                } else {
                    // QUEUED 也可能已交给 Worker，不能用旧执行态推断 RPC 未提交。
                    Log.error(TAG, "持久任务因宿主进程退出而中断[${schedule.name}] id=${schedule.id} state=${schedule.state}，保留业务待确认数据")
                    schedule.withFailure("host_process_restarted_unconfirmed", now)
                }
            } else schedule
        }
        if (recovered != schedules) save(recovered)
        recoveredExecutionsInProcess = true
    }

    fun activateSession(
        context: Context,
        ownerUserId: String,
        sessionEpoch: Long,
        now: Long = System.currentTimeMillis(),
    ) {
        val changed = withRegistryLock { activateSessionUnlocked(context, ownerUserId, sessionEpoch, now) }
        if (changed) SystemWakeScheduler.schedule(context, PersistentSchedule(), silent = true)
    }

    private fun activateSessionUnlocked(
        context: Context,
        ownerUserId: String,
        sessionEpoch: Long,
        now: Long = System.currentTimeMillis(),
    ): Boolean {
        if (!ensureStorage()) return false
        val safeOwnerUserId = ownerUserId.trim()
        if (
            safeOwnerUserId.isEmpty() ||
            sessionEpoch <= 0L ||
            !AccountSlotRegistry.isExecutableUser(safeOwnerUserId)
        ) {
            return false
        }
        val schedules = loadMutable()
        if (schedules.isEmpty()) return false
        val retained = mutableListOf<PersistentSchedule>()
        for (schedule in schedules) {
            val scheduleOwnerUserId = schedule.ownerUserId?.trim().orEmpty()
            val isCurrentSessionSchedule =
                scheduleOwnerUserId.isNotEmpty() &&
                    scheduleOwnerUserId == safeOwnerUserId &&
                    schedule.sessionEpoch == sessionEpoch
            if (!isCurrentSessionSchedule) {
                // 会话轮次换新只代表宿主进程重建；全局轮询/唤醒重绑到当前会话，
                // 直接丢弃会让下一次轮询根本没有计划可排（节点上就是「定时任务休眠」）。
                if (scheduleOwnerUserId == safeOwnerUserId &&
                    schedule.isGlobalKind() &&
                    schedule.state == PersistentScheduleState.SCHEDULED
                ) {
                    Log.record(
                        TAG,
                        "持久调度会话已换新，重绑到当前会话[${schedule.name}] ${schedule.sessionEpoch}->$sessionEpoch",
                    )
                    retained.add(
                        schedule.copy(
                            sessionEpoch = sessionEpoch,
                            updatedAtMs = now,
                            lastError = null,
                        ),
                    )
                } else {
                    StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(schedule.id) }
                }
                continue
            }

            if (schedule.state == PersistentScheduleState.SCHEDULED && schedule.triggerAtMs > now) {
                val prepared = PersistentLaunchPolicy.prepareScheduleForRegistration(context, schedule)
                if (prepared.blockedReason != null) {
                    StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(schedule.id) }
                    Log.record(TAG, "持久调度已因禁止系统调度前台拉起目标应用而停用[${schedule.name}]")
                    continue
                }
                retained.add(prepared.schedule)
            } else {
                StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(schedule.id) }
                retained.add(schedule)
            }
        }
        save(retained)
        return true
    }

    fun markFired(
        id: String,
        now: Long = System.currentTimeMillis(),
        source: String = "registry",
    ) {
        val schedule = get(id)
        val shouldDispatch = schedule?.kind == PersistentScheduleKind.MODULE_CHILD &&
            schedule.state !in terminalScheduleStates
        updateSchedule(id, source) { current ->
            if (current.state in terminalScheduleStates) current else current.withFired(now)
        }
        if (shouldDispatch) {
            ApplicationHookCore.dispatchIfNeeded()
        }
    }

    fun markFired(
        context: Context?,
        id: String,
        now: Long = System.currentTimeMillis(),
        source: String = "registry",
    ) {
        val schedule = get(id)
        markFired(id, now, source)
        if (context != null && schedule != null) {
            SystemWakeScheduler.schedule(context, schedule, silent = true)
        }
    }

    fun markQueued(
        context: Context?,
        id: String,
        now: Long = System.currentTimeMillis(),
        source: String = "registry",
    ): Boolean {
        val queued = withRegistryLock {
            val schedule = get(id) ?: return@withRegistryLock null
            if (schedule.state != PersistentScheduleState.SCHEDULED ||
                schedule.triggerAtMs > now || now > schedule.catchUpDeadlineAtMs() ||
                schedule.lastError == "launch_pending" || schedule.lastError == "delivery_pending"
            ) return@withRegistryLock null
            updateSchedule(id, source) { it.withQueued(now) }
            schedule
        } ?: return false
        context?.let { SystemWakeScheduler.schedule(it, queued, silent = true) }
        return true
    }

    fun markRunning(
        id: String,
        now: Long = System.currentTimeMillis(),
        source: String = "registry",
        expected: PersistentSchedule? = null,
    ): Boolean = withRegistryLock {
        val current = get(id) ?: return@withRegistryLock false
        if (expected != null && current != expected) return@withRegistryLock false
        if (current.state != PersistentScheduleState.QUEUED &&
            (expected != null || current.state != PersistentScheduleState.SCHEDULED)
        ) return@withRegistryLock false
        updateSchedule(id, source) { it.withRunning(now) }
        true
    }

    fun beginDeliveryWait(
        context: Context,
        expected: PersistentSchedule,
        launching: Boolean,
        now: Long = System.currentTimeMillis(),
    ): PersistentSchedule? {
        val waiting = withRegistryLock {
            val current = get(expected.id) ?: return@withRegistryLock null
            if (current != expected || current.state != PersistentScheduleState.SCHEDULED ||
                current.triggerAtMs > now || now > current.catchUpDeadlineAtMs()
            ) return@withRegistryLock null
            val waiting = current.copy(
                // 为启动/投递确认预留最多三十秒，避免未确认时反复拉起；不得跨过原任务的补执行窗口。
                // 已经迟到时仍保证不小于一秒的确认窗口，否则会被压到「现在」形成热循环。
                triggerAtMs = minOf(now + 30_000L, current.catchUpDeadlineAtMs())
                    .coerceAtLeast(now + 1_000L),
                lastFireAtMs = current.firstDueAtMs(),
                updatedAtMs = now,
                lastError = if (launching) "launch_pending" else "delivery_pending",
            )
            updateSchedule(current.id) { waiting }
            waiting
        } ?: return null
        if (!SystemWakeScheduler.schedule(context, waiting, silent = true)) {
            rescheduleDeferred(context, waiting.id, "confirmation_alarm_failed", now, expected = waiting)
            return null
        }
        return waiting
    }

    fun confirmTargetLaunch(
        id: String,
        confirmationAtMs: Long,
        now: Long = System.currentTimeMillis(),
    ): Boolean = withRegistryLock {
        val schedule = get(id) ?: return@withRegistryLock false
        if (schedule.state != PersistentScheduleState.SCHEDULED ||
            confirmationAtMs <= 0L || schedule.updatedAtMs != confirmationAtMs ||
            schedule.lastError !in setOf("launch_pending", "delivery_pending") ||
            now < schedule.firstDueAtMs() || now > schedule.catchUpDeadlineAtMs() ||
            !AccountSessionCoordinator.isScheduleRoutable(schedule)
        ) return@withRegistryLock false
        updateSchedule(id) {
            // 不能把触发时间拨回过去：AlarmManager 会把它压到「现在」，形成确认-重投热循环。
            it.copy(
                triggerAtMs = maxOf(it.firstDueAtMs(), now + 1_000L),
                updatedAtMs = now,
                lastError = null,
            )
        }
        true
    }

    fun rescheduleDeferred(
        context: Context?,
        id: String,
        reason: String,
        now: Long = System.currentTimeMillis(),
        expected: PersistentSchedule? = null,
    ): Boolean {
        if (id.isBlank() || context == null) return false
        var exhausted = false
        val updated = withRegistryLock {
            val schedule = get(id) ?: return@withRegistryLock null
            if (expected != null && schedule != expected) return@withRegistryLock null
            if (schedule.state !in setOf(PersistentScheduleState.SCHEDULED, PersistentScheduleState.QUEUED)) {
                return@withRegistryLock null
            }
            val nextAttempt = schedule.attemptCount + 1
            // 业务子任务按 15/30/60 秒递增退避；全局轮询/唤醒放宽到 5 分钟一档，
            // 否则两次失败后的第三次就可能耗光次数，把整条轮询链打成终态。
            val delayMs = when (nextAttempt) {
                1 -> 15_000L
                2 -> 30_000L
                else -> if (schedule.isGlobalKind()) GLOBAL_DEFER_BACKOFF_MS else 60_000L
            }
            val attemptLimit =
                if (schedule.isGlobalKind()) MAX_GLOBAL_DEFERRED_ATTEMPTS else MAX_DEFERRED_ATTEMPTS
            val nextTriggerAt = now + delayMs
            exhausted = nextAttempt > attemptLimit || nextTriggerAt > schedule.catchUpDeadlineAtMs()
            val updated = if (exhausted) schedule.withFailure("deferred_exhausted:$reason", now)
                else schedule.withDeferredRetry(nextTriggerAt, reason, now)
            updateSchedule(id) { updated }
            updated
        } ?: return false
        if (exhausted) {
            SystemWakeScheduler.schedule(context, updated, silent = true)
            if (updated.kind == PersistentScheduleKind.MODULE_CHILD) ApplicationHookCore.dispatchIfNeeded()
            return false
        }
        if (SystemWakeScheduler.schedule(context, updated)) {
            Log.record(
                TAG,
                "持久调度延后重试[${updated.name}] attempt=${updated.attemptCount} at=${TimeUtil.getCommonDate(updated.triggerAtMs)} reason=$reason",
            )
            return true
        }
        markFailed(context, id, "deferred_alarm_schedule_failed:$reason", now, expected = updated)
        return false
    }

    fun markFailed(
        id: String,
        error: String,
        now: Long = System.currentTimeMillis(),
        source: String = "registry",
    ) {
        val schedule = get(id)
        val shouldDispatch = schedule?.kind == PersistentScheduleKind.MODULE_CHILD &&
            schedule.state !in terminalScheduleStates
        updateSchedule(id, source) { current ->
            if (current.state in terminalScheduleStates) current else current.withFailure(error, now)
        }
        if (shouldDispatch) {
            ApplicationHookCore.dispatchIfNeeded()
        }
    }

    fun markWorkerFailedIfActive(
        context: Context?,
        id: String,
        error: String,
        now: Long = System.currentTimeMillis(),
        source: String = "worker_completion",
    ): Boolean {
        val schedule = get(id)
        var changed = false
        updateSchedule(id, source) { current ->
            if (current.state in activeModuleChildStates) {
                changed = true
                current.withFailure(error, now)
            } else {
                current
            }
        }
        if (changed) {
            if (context != null && schedule != null) {
                SystemWakeScheduler.schedule(context, schedule, silent = true)
            }
            ApplicationHookCore.dispatchIfNeeded()
        }
        return changed
    }

    fun markFailed(
        context: Context?,
        id: String,
        error: String,
        now: Long = System.currentTimeMillis(),
        source: String = "registry",
        expected: PersistentSchedule? = null,
    ) {
        val failed = withRegistryLock {
            val schedule = get(id) ?: return@withRegistryLock null
            if (expected != null && schedule != expected) return@withRegistryLock null
            updateSchedule(id, source) { current ->
                if (current.state in terminalScheduleStates) current else current.withFailure(error, now)
            }
            schedule
        } ?: return
        if (context != null) SystemWakeScheduler.schedule(context, failed, silent = true)
        if (failed.kind == PersistentScheduleKind.MODULE_CHILD && failed.state !in terminalScheduleStates) {
            ApplicationHookCore.dispatchIfNeeded()
        }
    }

    fun markExpired(
        context: Context?,
        id: String,
        now: Long = System.currentTimeMillis(),
        source: String = "registry",
    ) {
        val schedule = get(id)
        val shouldDispatch = schedule?.kind == PersistentScheduleKind.MODULE_CHILD &&
            schedule.state !in terminalScheduleStates
        updateSchedule(id, source) { current ->
            if (current.state == PersistentScheduleState.SCHEDULED && now > current.deadlineAtMs()) {
                current.withScheduleState(PersistentScheduleState.EXPIRED, now)
            } else current
        }
        if (context != null && schedule != null) {
            SystemWakeScheduler.schedule(context, schedule, silent = true)
        }
        if (shouldDispatch) {
            ApplicationHookCore.dispatchIfNeeded()
        }
    }

    fun reconcile(
        context: Context,
        now: Long = System.currentTimeMillis(),
        mode: PersistentReconcileMode = PersistentReconcileMode.RESCHEDULE_ONLY,
    ): ReconcileResult {
        val result = withRegistryLock { reconcileUnlocked(context, now, mode) }
            ?: return ReconcileResult(emptyList(), 0, 0)
        val replanSucceeded = SystemWakeScheduler.schedule(context, PersistentSchedule(), silent = true)
        return result.copy(rescheduledCount = if (replanSucceeded) result.rescheduledCount else 0)
    }

    private fun reconcileUnlocked(
        context: Context,
        now: Long = System.currentTimeMillis(),
        mode: PersistentReconcileMode = PersistentReconcileMode.RESCHEDULE_ONLY,
    ): ReconcileResult? {
        if (!ensureStorage()) return null
        val schedules = loadMutable()
        if (schedules.isEmpty()) return null
        val activeSession =
            AccountSessionCoordinator.currentOrPersistedSessionIdentity() ?: run {
                Log.record(TAG, "当前无可恢复会话，跳过持久调度恢复重排")
                return null
            }
        val due = mutableListOf<PersistentSchedule>()
        var expired = 0
        val retained = mutableListOf<PersistentSchedule>()

        for (schedule in schedules) {
            val ownerUserId = schedule.ownerUserId?.trim().orEmpty()
            val isCurrentSessionSchedule =
                ownerUserId.isNotEmpty() &&
                    ownerUserId == activeSession.userId &&
                    schedule.sessionEpoch == activeSession.sessionEpoch

            if (schedule.state != PersistentScheduleState.SCHEDULED) {
                StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(schedule.id) }
                if (isCurrentSessionSchedule && now - schedule.updatedAtMs <= RETAIN_FINISHED_MS) {
                    retained.add(schedule)
                }
                continue
            }

            if (!isCurrentSessionSchedule) {
                // 恢复广播/开机后会话轮次通常已经换新；全局轮询/唤醒重绑而不是静默丢弃，
                // 否则整个进程生命周期内都不会再有轮询计划。
                if (ownerUserId == activeSession.userId &&
                    schedule.isGlobalKind() &&
                    schedule.triggerAtMs > now - PersistentScheduleDefaults.GLOBAL_CATCH_UP_WINDOW_MS
                ) {
                    Log.record(
                        TAG,
                        "持久调度会话已换新，重绑到当前会话[${schedule.name}] ${schedule.sessionEpoch}->${activeSession.sessionEpoch}",
                    )
                    retained.add(
                        schedule.copy(
                            sessionEpoch = activeSession.sessionEpoch,
                            updatedAtMs = now,
                        ),
                    )
                } else {
                    StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(schedule.id) }
                }
                continue
            }

            if (schedule.triggerAtMs <= now) {
                if (now <= schedule.catchUpDeadlineAtMs()) {
                    if (mode == PersistentReconcileMode.FIRE_ALARM_DUE) {
                        due.add(schedule)
                        retained.add(schedule)
                        Log.record(TAG, "发现到期持久任务[${schedule.name}] ${TimeUtil.getCommonDate(schedule.triggerAtMs)}")
                    } else {
                        retained.add(schedule)
                        Log.runtime(TAG, "恢复重排保留窗口内到期任务[${schedule.name}] ${TimeUtil.getCommonDate(schedule.triggerAtMs)}")
                    }
                } else {
                    expired++
                    StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(schedule.id) }
                    retained.add(schedule.withScheduleState(PersistentScheduleState.EXPIRED, now))
                    Log.record(TAG, "持久任务已过期[${schedule.name}] ${TimeUtil.getCommonDate(schedule.triggerAtMs)}")
                }
                continue
            }

            val prepared = PersistentLaunchPolicy.prepareScheduleForRegistration(context, schedule)
            if (prepared.blockedReason != null) {
                StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(schedule.id) }
                Log.record(TAG, "持久调度已因禁止系统调度前台拉起目标应用而停用[${schedule.name}]")
                continue
            }
            retained.add(prepared.schedule)
        }

        save(retained)
        return ReconcileResult(
            dueSchedules = due,
            rescheduledCount = if (retained.any { it.state == PersistentScheduleState.SCHEDULED }) 1 else 0,
            expiredCount = expired,
        )
    }

    private fun isSameScheduledTask(
        left: PersistentSchedule,
        right: PersistentSchedule,
    ): Boolean =
        left.state == PersistentScheduleState.SCHEDULED &&
            right.state == PersistentScheduleState.SCHEDULED &&
            left.name == right.name &&
            left.kind == right.kind &&
            left.triggerAtMs == right.triggerAtMs &&
            left.toleranceMs == right.toleranceMs &&
            left.effectivePrecisionPolicy() == right.effectivePrecisionPolicy() &&
            left.dedupeKey == right.dedupeKey &&
            canonicalPayloadJson(left.payloadJson) == canonicalPayloadJson(right.payloadJson) &&
            left.ownerUserId?.trim().orEmpty() == right.ownerUserId?.trim().orEmpty() &&
            left.sessionEpoch == right.sessionEpoch

    private fun canonicalPayloadJson(payloadJson: String): String {
        val trimmed = payloadJson.trim().ifBlank { "{}" }
        return try {
            canonicalJsonValue(JSONObject(trimmed))
        } catch (_: Throwable) {
            trimmed
        }
    }

    private fun canonicalJsonValue(value: Any?): String =
        when (value) {
            null, JSONObject.NULL -> {
                "null"
            }

            is JSONObject -> {
                val keys = mutableListOf<String>()
                val iterator = value.keys()
                while (iterator.hasNext()) {
                    keys.add(iterator.next())
                }
                keys.sorted().joinToString(prefix = "{", postfix = "}") { key ->
                    JSONObject.quote(key) + ":" + canonicalJsonValue(value.opt(key))
                }
            }

            is JSONArray -> {
                (0 until value.length()).joinToString(prefix = "[", postfix = "]") { index ->
                    canonicalJsonValue(value.opt(index))
                }
            }

            is String -> {
                JSONObject.quote(value)
            }

            is Number,
            is Boolean,
            -> {
                value.toString()
            }

            else -> {
                JSONObject.quote(value.toString())
            }
        }

    private fun updateSchedule(
        id: String,
        source: String = "registry",
        updater: (PersistentSchedule) -> PersistentSchedule,
    ) {
        if (id.isBlank()) return
        withRegistryLock {
            if (!ensureStorage()) return@withRegistryLock
            val schedules = loadMutable()
            val index = schedules.indexOfFirst { it.id == id }
            if (index < 0) return@withRegistryLock
            val previous = schedules[index]
            val updated = updater(previous)
            if (updated == previous) return@withRegistryLock
            schedules[index] = updated
            save(schedules)
            if (previous.state != updated.state) {
                Log.record(
                    TAG,
                    "持久调度状态变更[id=${updated.id}] ${previous.state}->${updated.state} kind=${updated.kind} source=$source owner=${updated.ownerUserId} session=${updated.sessionEpoch}",
                )
            }
            if (previous.state == PersistentScheduleState.SCHEDULED &&
                (updated.state != PersistentScheduleState.SCHEDULED || updated.triggerAtMs != previous.triggerAtMs)
            ) {
                StoreAccess.afterTransaction { SystemWakeScheduler.cancelLaunchConfirmationTimeout(previous.id) }
            }
        }
    }

    private fun loadMutable(): MutableList<PersistentSchedule> {
        if (!ensureStorage()) return mutableListOf()
        cache?.let { return it.toMutableList() }
        return StoreAccess.transaction {
            cache?.let { return@transaction it.toMutableList() }
            val loaded = try {
                DataStore.getOrCreate(STORE_KEY, scheduleListType)
            } catch (t: Throwable) {
                Log.printStackTrace(TAG, "读取持久调度列表失败，中止本次操作", t)
                throw t
            }
            cache = loaded.toList()
            loaded.toMutableList()
        }
    }

    private fun save(schedules: List<PersistentSchedule>) {
        check(ensureStorage()) { "persistent_storage_unavailable" }
        val snapshot = schedules.toList()
        DataStore.put(STORE_KEY, snapshot)
        cache = snapshot
        StoreAccess.afterTransaction { SystemWakeScheduler.onRegistryChanged() }
    }

    private fun ensureStorage(): Boolean {
        if (storageReady) return true
        return StoreAccess.transaction {
            if (storageReady) return@transaction true
            val initialized =
                runCatching { DataStore.init(Files.CONFIG_DIR) }
                    .onFailure { Log.printStackTrace(TAG, "初始化持久调度存储失败", it) }
                    .isSuccess &&
                    Files.CONFIG_DIR.exists() &&
                    Files.child(Files.CONFIG_DIR, "DataStore.json").exists()
            if (initialized) {
                // 跨进程：当底层 DataStore 文件被其它进程改动并重载时，失效本地缓存，
                // 下次读取重新从 DataStore 取最新数据，避免缓存陈旧导致调度丢失/误取消。
                runCatching {
                    DataStore.setOnChangeListener {
                        invalidateCache()
                        SystemWakeScheduler.onRegistryChanged()
                    }
                }
                storageReady = true
            }
            initialized
        }
    }

    internal fun <T> withRegistryLock(block: () -> T): T = StoreAccess.transaction {
        if ((registryLockDepth.get() ?: 0) > 0) return@transaction block()
        check(ensureStorage()) { "persistent_storage_unavailable" }
        registryLockDepth.set(1)
        try { invalidateCache(); block() }
        finally { registryLockDepth.remove() }
    }

    private fun invalidateCache() {
        cache = null
    }
}
