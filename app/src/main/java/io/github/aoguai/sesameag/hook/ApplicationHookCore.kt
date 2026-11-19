package io.github.aoguai.sesameag.hook

import io.github.aoguai.sesameag.hook.ApplicationHookConstants.TriggerInfo
import io.github.aoguai.sesameag.hook.ApplicationHookConstants.TriggerType
import io.github.aoguai.sesameag.hook.keepalive.GlobalChainWatchdog
import io.github.aoguai.sesameag.hook.keepalive.PersistentScheduleRegistry
import io.github.aoguai.sesameag.util.Log.printStackTrace
import io.github.aoguai.sesameag.util.Log.record
import java.util.concurrent.atomic.AtomicBoolean

object ApplicationHookCore {
    private const val TAG = "ApplicationHookCore"

    // 触发队列有容量上限，但入口协程没有；只保留一个检查工作并合并并发请求。
    private val dispatchScheduled = AtomicBoolean(false)
    private val dispatchRequested = AtomicBoolean(false)

    /** 全局触发排空：全局轮询/唤醒不会被主任务消费，必须单独清账，否则会永久占住触发队列。 */
    private val drainInFlight = AtomicBoolean(false)

    /** 派发停摆的起点；0 表示当前不存在停摆。 */
    @Volatile
    private var stallSinceMs = 0L
    private const val STALL_RELEASE_MS = 5 * 60 * 1000L
    @Volatile
    private var drainScheduledAtMs = 0L
    private const val DRAIN_RETRY_GAP_MS = 15 * 1000L

    fun requestExecution(trigger: TriggerInfo): Boolean {
        val boundTrigger = AccountSessionCoordinator.bindTrigger(trigger)
        if (!AccountSessionCoordinator.shouldAcceptTrigger(boundTrigger)) {
            record(TAG, "ignore trigger due to stale/switching session: ${boundTrigger.summary()}")
            return false
        }
        val queueResult = ApplicationHookConstants.setPendingTrigger(boundTrigger)
        // 队列锁外收尾；同一个持久 ID 的重复入队不能提前结束其执行状态。
        queueResult.replaced.forEach { previous ->
            val scheduleId = previous.persistentScheduleId?.takeIf { it.isNotBlank() }
                ?: return@forEach
            if (scheduleId == boundTrigger.persistentScheduleId) return@forEach
            val covered = previous.type in setOf(TriggerType.ALARM_POLL, TriggerType.ALARM_WAKEUP) &&
                previous.type == boundTrigger.type &&
                previous.ownerUserId == boundTrigger.ownerUserId &&
                previous.sessionEpoch == boundTrigger.sessionEpoch &&
                previous.alarmTriggered == boundTrigger.alarmTriggered &&
                previous.wakenAtTime == boundTrigger.wakenAtTime &&
                previous.wakenTime == boundTrigger.wakenTime
            if (covered) {
                PersistentScheduleRegistry.markFired(
                    ApplicationHook.appContext, scheduleId, source = "trigger_coalesced",
                )
            } else {
                PersistentScheduleRegistry.rescheduleDeferred(
                    ApplicationHook.appContext, scheduleId, "trigger_replaced_without_coverage",
                )
            }
        }
        queueResult.displaced
            ?.persistentScheduleId
            ?.takeIf { it.isNotBlank() }
            ?.let { scheduleId ->
                // setPendingTrigger 已退出 triggerLock；持久化和 AlarmManager 操作不得在队列锁内执行。
                PersistentScheduleRegistry.rescheduleDeferred(
                    ApplicationHook.appContext,
                    scheduleId,
                    "trigger_queue_full",
                )
            }
        dispatchIfNeeded()
        return queueResult.accepted
    }

    fun dispatchIfNeeded() {
        dispatchRequested.set(true)
        scheduleDispatchIfPossible()
        scheduleGlobalTriggerDrain()
        detectDispatchStall()
    }

    /**
     * 派发停摆检测。
     *
     * 只要队列里有没被消费的普通触发、主任务没在跑、也没有活跃的业务子任务，
     * 正常的派发路径就该立刻把它拿走。若这种状态持续 [STALL_RELEASE_MS]，说明某个门禁
     * 一直没开放（实证：全局触发占着队列、主任务被离线中断后无人再派发），
     * 此时把全局触发清掉并补登记，让主流程重新拿到一次执行机会。
     */
    private fun detectDispatchStall() {
        val now = System.currentTimeMillis()
        val pending = ApplicationHookConstants.pendingTriggerCount()
        val mainRunning = ApplicationHook.mainTask?.isRunning == true
        val childActive = PersistentScheduleRegistry.hasActiveModuleChild(
            AccountSessionCoordinator.currentUserId(),
            AccountSessionCoordinator.currentSessionEpoch(),
        )
        val eligible = pending > 0 && !mainRunning && !childActive
        if (!eligible) {
            stallSinceMs = 0L
            return
        }
        if (stallSinceMs == 0L) {
            stallSinceMs = now
            return
        }
        if (now - stallSinceMs < STALL_RELEASE_MS) return
        stallSinceMs = 0L
        record(
            TAG,
            "派发停摆 $STALL_RELEASE_MS ms，主动清场并补一次执行: pending=$pending " +
                ApplicationHook.readinessSummary(),
        )
        scheduleGlobalTriggerDrain()
        // 停摆恢复只负责清场并补登记；真正的派发仍走那条经过各道门禁的路径，
        // 这里直接调 scheduleDispatchIfPossible 会套娃回 dispatchIfNeeded 自己。
        releaseStalledDispatch()
    }

    private fun releaseStalledDispatch() {
        val drained = ApplicationHookConstants.removePendingTriggers("dispatch_stall_release") { trigger ->
            trigger.type == TriggerType.ALARM_POLL ||
                (trigger.type == TriggerType.ALARM_WAKEUP && !trigger.wakenAtTime)
        }
        drained.forEach { trigger ->
            val scheduleId = trigger.persistentScheduleId?.takeIf { it.isNotBlank() } ?: return@forEach
            PersistentScheduleRegistry.onGlobalTriggerDrained(
                context = ApplicationHook.appContext,
                scheduleId = scheduleId,
                registerNextPoll = { ApplicationHook.reRegisterPollSchedule() },
            )
        }
        record(TAG, "派发停摆清场完成 drained=${drained.size} pending=${ApplicationHookConstants.pendingTriggerCount()}")
    }

    private fun scheduleDispatchIfPossible() {
        if (!canScheduleDispatch() || !dispatchScheduled.compareAndSet(false, true)) {
            return
        }
        ApplicationHookConstants.submitEntry("dispatch_pending_triggers") {
            try {
                dispatchRequested.set(false)
                dispatchPendingTriggers()
            } finally {
                dispatchScheduled.set(false)
                if (dispatchRequested.get()) {
                    scheduleDispatchIfPossible()
                }
            }
        }
    }

    /**
     * 排空无法由主任务执行的全局触发。
     *
     * 全局轮询/唤醒的作用只是「到点了」：真正要跑的下一轮轮询由 [GlobalChainWatchdog] 与
     * TaskRunner 结束时的预登记负责，它们本身没有主任务可以消费。只要它们一直躺在队列里，
     * [canScheduleDispatch] 就永远为真、[dispatchPendingTriggers] 就会一直走到
     * 「主任务在跑/队列非空」的提前返回，而队列里的东西又永远没人拿走——
     * 实证里表现为 09:30 的 ALARM_POLL 一直挂到 10:51 才被超时回收，期间一次主任务都没派发。
     */
    private fun scheduleGlobalTriggerDrain() {
        val now = System.currentTimeMillis()
        if (now - drainScheduledAtMs < DRAIN_RETRY_GAP_MS) return
        if (ApplicationHook.mainTask?.isRunning == true) return
        if (!drainInFlight.compareAndSet(false, true)) return
        drainScheduledAtMs = now
        ApplicationHookConstants.submitEntry("drain_global_triggers") {
            try {
                drainGlobalTriggers()
            } finally {
                drainInFlight.set(false)
            }
        }
    }

    private fun drainGlobalTriggers() {
        val drained = ApplicationHookConstants.removePendingTriggers("global_trigger_not_consumable") { trigger ->
            // 只清「纯信号」：带 wakenAtTime 的唤醒要参与跨日刷新与唤醒链，仍交给正常派发路径。
            trigger.type == TriggerType.ALARM_POLL ||
                (trigger.type == TriggerType.ALARM_WAKEUP && !trigger.wakenAtTime)
        }
        if (drained.isEmpty()) return
        drained.forEach { trigger ->
            val scheduleId = trigger.persistentScheduleId?.takeIf { it.isNotBlank() } ?: return@forEach
            // 只标完成会留下空窗：这两类记录不会被执行，必须顺手补登记下一轮。
            PersistentScheduleRegistry.onGlobalTriggerDrained(
                context = ApplicationHook.appContext,
                scheduleId = scheduleId,
                registerNextPoll = { ctx -> ApplicationHook.reRegisterPollSchedule() },
            )
        }
        record(TAG, "🧹 已排空不可执行的全局触发 ${drained.size} 个，pending=${ApplicationHookConstants.pendingTriggerCount()}")
        // 排空后真正该跑的任务才有机会派发。
        dispatchIfNeeded()
    }

    private fun canScheduleDispatch(): Boolean {
        if (!ApplicationHookConstants.hasPendingTriggers()) {
            return false
        }
        if (ApplicationHook.mainTask?.isRunning == true) {
            return false
        }
        return !PersistentScheduleRegistry.hasActiveModuleChild(
            AccountSessionCoordinator.currentUserId(),
            AccountSessionCoordinator.currentSessionEpoch(),
        )
    }

    private fun dispatchPendingTriggers() {
        ApplicationHook.discardCoveredAlarmPollTriggers()
        ApplicationHookConstants.removePendingTriggers("stale_session_trigger") { trigger ->
            !AccountSessionCoordinator.shouldAcceptTrigger(trigger)
        }
        if (!ApplicationHookConstants.hasPendingTriggers()) return

        if (AccountSessionCoordinator.isSwitching()) {
            record(TAG, "session switching, skip dispatch: ${ApplicationHook.readinessSummary()}")
            return
        }

        if (ApplicationHookConstants.isOffline()) {
            record(TAG, "offline active, skip dispatch: ${ApplicationHook.readinessSummary()}")
            return
        }

        val mainTask = ApplicationHook.mainTask
        if (mainTask == null) {
            record(TAG, "mainTask is null: ${ApplicationHook.readinessSummary()}")
            return
        }

        if (!ApplicationHook.isReadyForExec()) {
            record(TAG, "not ready for exec: ${ApplicationHook.readinessSummary()}")
            return
        }

        if (mainTask.isRunning) {
            record(TAG, "mainTask is running, pending=${ApplicationHookConstants.pendingTriggerCount()}")
            return
        }

        val currentOwnerUserId = AccountSessionCoordinator.currentUserId()
        val currentSessionEpoch = AccountSessionCoordinator.currentSessionEpoch()
        if (PersistentScheduleRegistry.hasActiveModuleChild(currentOwnerUserId, currentSessionEpoch)) {
            record(
                TAG,
                "module persistent child is active, defer mainTask: pending=${ApplicationHookConstants.pendingTriggerCount()} owner=$currentOwnerUserId session=$currentSessionEpoch",
            )
            return
        }

        record(TAG, "▶️ dispatch mainTask, pending=${ApplicationHookConstants.pendingTriggerCount()}")
        val job = mainTask.startTask(force = false, rounds = 1)
        job.invokeOnCompletion {
            // 主任务结束时收敛残局：这条链断了就补登记，否则一次取消就能让轮询永久停摆。
            GlobalChainWatchdog.ensureAlive(
                context = ApplicationHook.appContext,
                ownerUserId = currentOwnerUserId,
                sessionEpoch = currentSessionEpoch,
            ) { ctx -> ApplicationHook.reRegisterPollSchedule() }
            dispatchIfNeeded()
        }
    }
}
